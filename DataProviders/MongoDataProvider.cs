using LinqToDB;
using LinqToDB.Data;
using LinqToDB.DataProvider;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using Q1.RedisSyncDataFromDB.Service.DataProviders.MongoService;
using System.Linq;
using System.Linq.Expressions;

namespace Q1.RedisSyncDataFromDB.Service.DataProviders
{
    /// <summary>
    /// Represents the MS SQL Server data provider
    /// </summary>
    public partial class MongoDataProvider : BaseDataProvider, ICustomDataProvider
    {
        #region Fields

        private static readonly Lazy<IMongoDbService> _dataProvider = new(() => new MongoDbService(_customConnectionStringInfo.DBConnectionString, _customConnectionStringInfo.DataBase), true);

        private static ICustomConnectionInfo _customConnectionStringInfo;

        #endregion

        #region Ctor

        public MongoDataProvider(ICustomConnectionInfo customConnectionStringInfo) : base(customConnectionStringInfo)
        {
            _customConnectionStringInfo = customConnectionStringInfo;
        }

        #endregion

        #region Utils

        /// <summary>
        /// Gets a connection to the database for a current data provider
        /// </summary>
        /// <param name="connectionInfo">Connection string</param>
        /// <returns>Connection to a database</returns>
        protected override DbConnection GetInternalDbConnection(string connectionInfo)
        {
            return null;
        }

        #endregion

        #region Methods

        /// <summary>
        /// Create the database
        /// </summary>
        /// <param name="collation">Collation</param>
        /// <param name="triesToConnect">Count of tries to connect to the database after creating; set 0 if no need to connect after creating</param>
        public void CreateDatabase(string collation, int triesToConnect = 10)
        {
            if (DatabaseExists())
                return;


        }

        /// <summary>
        /// Checks if the specified database exists, returns true if database exists
        /// </summary>
        /// <returns>
        /// A task that represents the asynchronous operation
        /// The task result contains the returns true if the database exists.
        /// </returns>
        public async Task<bool> DatabaseExistsAsync()
        {
            try
            {
                await using var connection = GetInternalDbConnection(await GetCurrentConnectionStringAsync());

                //just try to connect
                await connection.OpenAsync();

                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Checks if the specified database exists, returns true if database exists
        /// </summary>
        /// <returns>Returns true if the database exists.</returns>
        public bool DatabaseExists()
        {
            try
            {
                using var connection = GetInternalDbConnection(GetCurrentConnectionString());
                //just try to connect
                connection.Open();

                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Get the current identity value
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <returns>
        /// A task that represents the asynchronous operation
        /// The task result contains the integer identity; null if cannot get the result
        /// </returns>


        /// <summary>
        /// Creates a backup of the database
        /// </summary>
        /// <returns>A task that represents the asynchronous operation</returns>
        public virtual async Task BackupDatabaseAsync(string fileName)
        {
            using var currentConnection = await CreateDataConnectionAsync();
            var commandText = $"BACKUP DATABASE [{currentConnection.Connection.Database}] TO DISK = '{fileName}' WITH FORMAT";
            await currentConnection.ExecuteAsync(commandText);
        }

        /// <summary>
        /// Restores the database from a backup
        /// </summary>
        /// <param name="backupFileName">The name of the backup file</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        public virtual async Task RestoreDatabaseAsync(string backupFileName)
        {
            using var currentConnection = await CreateDataConnectionAsync();
            var commandText = string.Format(
                "DECLARE @ErrorMessage NVARCHAR(4000)\n" +
                "ALTER DATABASE [{0}] SET OFFLINE WITH ROLLBACK IMMEDIATE\n" +
                "BEGIN TRY\n" +
                "RESTORE DATABASE [{0}] FROM DISK = '{1}' WITH REPLACE\n" +
                "END TRY\n" +
                "BEGIN CATCH\n" +
                "SET @ErrorMessage = ERROR_MESSAGE()\n" +
                "END CATCH\n" +
                "ALTER DATABASE [{0}] SET MULTI_USER WITH ROLLBACK IMMEDIATE\n" +
                "IF (@ErrorMessage is not NULL)\n" +
                "BEGIN\n" +
                "RAISERROR (@ErrorMessage, 16, 1)\n" +
                "END",
                currentConnection.Connection.Database,
                backupFileName);

            await currentConnection.ExecuteAsync(commandText);
        }

        /// <summary>
        /// Re-index database tables
        /// </summary>
        /// <returns>A task that represents the asynchronous operation</returns>
        public virtual async Task ReIndexTablesAsync()
        {
            using var currentConnection = await CreateDataConnectionAsync();
            var commandText = $@"
                    DECLARE @TableName sysname 
                    DECLARE cur_reindex CURSOR FOR
                    SELECT table_name
                    FROM [{currentConnection.Connection.Database}].information_schema.tables
                    WHERE table_type = 'base table'
                    OPEN cur_reindex
                    FETCH NEXT FROM cur_reindex INTO @TableName
                    WHILE @@FETCH_STATUS = 0
                        BEGIN
                            exec('ALTER INDEX ALL ON [' + @TableName + '] REBUILD')
                            FETCH NEXT FROM cur_reindex INTO @TableName
                        END
                    CLOSE cur_reindex
                    DEALLOCATE cur_reindex";

            await currentConnection.ExecuteAsync(commandText);
        }

        /// <summary>
        /// Build the connection string
        /// </summary>
        /// <param name="connectionInfo">Connection string info</param>
        /// <returns>Connection string</returns>
        public virtual string BuildConnectionString(ICustomConnectionInfo connectionInfo)
        {
            if (connectionInfo is null)
                throw new ArgumentNullException(nameof(connectionInfo));

            return connectionInfo.DBConnectionString;
        }

        /// <summary>
        /// Gets the name of a foreign key
        /// </summary>
        /// <param name="foreignTable">Foreign key table</param>
        /// <param name="foreignColumn">Foreign key column name</param>
        /// <param name="primaryTable">Primary table</param>
        /// <param name="primaryColumn">Primary key column name</param>
        /// <returns>Name of a foreign key</returns>
        public virtual string CreateForeignKeyName(string foreignTable, string foreignColumn, string primaryTable, string primaryColumn)
        {
            return $"FK_{foreignTable}_{foreignColumn}_{primaryTable}_{primaryColumn}";
        }

        /// <summary>
        /// Gets the name of an index
        /// </summary>
        /// <param name="targetTable">Target table name</param>
        /// <param name="targetColumn">Target column name</param>
        /// <returns>Name of an index</returns>
        public virtual string GetIndexName(string targetTable, string targetColumn)
        {
            return $"IX_{targetTable}_{targetColumn}";
        }

        /// <summary>
        /// Updates records in table, using values from entity parameter. 
        /// Records to update are identified by match on primary key value from obj value.
        /// </summary>
        /// <param name="entities">Entities with data to update</param>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <returns>A task that represents the asynchronous operation</returns>
        public override async Task UpdateEntitiesAsync<TEntity>(IEnumerable<TEntity> entities)
        {
            using var dataContext = await CreateDataConnectionAsync();
            await dataContext.GetTable<TEntity>()
                .Merge()
                .Using(entities)
                .OnTargetKey()
                .UpdateWhenMatched()
                .MergeAsync();
        }

        public override async Task<IList<T>> QueryAsync<T>(string sql, Expression<Func<T, bool>> query, params DataParameter[] parameters)
        {
            return await _dataProvider.Value.GetCollection<T>(sql).AsQueryable().Where(query).ToListAsync(CancellationToken.None);
        }
        public override async Task<TEntity> InsertEntityAsync<TEntity>(TEntity entity)
        {
            await _dataProvider.Value.GetCollection<TEntity>(entity.GetType().Name).InsertOneAsync(entity);
            return entity;
        }
        #endregion

        #region Properties

        /// <summary>
        /// Sql server data provider
        /// </summary>
        protected override IDataProvider LinqToDbDataProvider => _dataProvider.Value as IDataProvider;

        /// <summary>
        /// Gets allowed a limit input value of the data for hashing functions, returns 0 if not limited
        /// </summary>
        public int SupportedLengthOfBinaryHash { get; } = 8000;

        /// <summary>
        /// Gets a value indicating whether this data provider supports backup
        /// </summary>
        public virtual bool BackupSupported => true;

        #endregion
    }
}
