using LinqToDB;
using LinqToDB.Common;
using LinqToDB.Data;
using LinqToDB.DataProvider;
using LinqToDB.SqlQuery;
using Q1.RedisSyncDataFromDB.Service.DataProviders.LinqToDB;
using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Q1.RedisSyncDataFromDB.Service.Entitys;

namespace Q1.RedisSyncDataFromDB.Service.DataProviders
{
    public class PostgreSqlDataProvider : BaseDataProvider, ICustomDataProvider
    {
        #region Fields

        private static readonly Lazy<IDataProvider> _dataProvider = new(() => new LinqToDBPostgreSQLDataProvider(), true);

        #endregion

        #region Ctor

        public PostgreSqlDataProvider(ICustomConnectionInfo customConnectionStringInfo) : base(customConnectionStringInfo)
        {
        }

        #endregion

        #region Utils

        /// <summary>
        /// Creates the database connection by the current data configuration
        /// </summary>
        protected override DataConnection CreateDataConnection()
        {
            var dataContext = CreateDataConnection(LinqToDbDataProvider);
            dataContext.MappingSchema.SetDataType(
                typeof(string),
                new SqlDataType(new DbDataType(typeof(string), "citext")));

            return dataContext;
        }

        protected NpgsqlConnectionStringBuilder GetConnectionStringBuilder()
        {
            return new NpgsqlConnectionStringBuilder(GetCurrentConnectionString());
        }

        /// <summary>
        /// Gets a connection to the database for a current data provider
        /// </summary>
        /// <param name="connectionInfo">Connection string</param>
        /// <returns>Connection to a database</returns>
        protected override DbConnection GetInternalDbConnection(string connectionInfo)
        {
            if (string.IsNullOrEmpty(connectionInfo))
                throw new ArgumentException(nameof(connectionInfo));

            return new NpgsqlConnection(connectionInfo);
        }

        #endregion

        #region Methods

        /// <summary>
        /// Creates the database by using the loaded connection string
        /// </summary>
        /// <param name="collation"></param>
        /// <param name="triesToConnect"></param>
        public void CreateDatabase(string collation, int triesToConnect = 10)
        {
            if (DatabaseExists())
                return;

            var builder = GetConnectionStringBuilder();

            //gets database name
            var databaseName = builder.Database;

            //now create connection string to 'postgres' - default administrative connection database.
            builder.Database = "postgres";

            using (var connection = GetInternalDbConnection(builder.ConnectionString))
            {
                var query = $"CREATE DATABASE \"{databaseName}\" WITH OWNER = '{builder.Username}'";
                if (!string.IsNullOrWhiteSpace(collation))
                    query = $"{query} LC_COLLATE = '{collation}'";

                var command = connection.CreateCommand();
                command.CommandText = query;
                command.Connection.Open();

                command.ExecuteNonQuery();
            }

            //try connect
            if (triesToConnect <= 0)
                return;

            //sometimes on slow servers (hosting) there could be situations when database requires some time to be created.
            //but we have already started creation of tables and sample data.
            //as a result there is an exception thrown and the installation process cannot continue.
            //that's why we are in a cycle of "triesToConnect" times trying to connect to a database with a delay of one second.
            for (var i = 0; i <= triesToConnect; i++)
            {
                if (i == triesToConnect)
                    throw new Exception("Unable to connect to the new database. Please try one more time");

                if (!DatabaseExists())
                    Thread.Sleep(1000);
                else
                {
                    builder.Database = databaseName;
                    using var connection = GetInternalDbConnection(builder.ConnectionString) as NpgsqlConnection;
                    var command = connection.CreateCommand();
                    command.CommandText = "CREATE EXTENSION IF NOT EXISTS citext; CREATE EXTENSION IF NOT EXISTS pgcrypto;";
                    command.Connection.Open();
                    command.ExecuteNonQuery();
                    connection.ReloadTypes();

                    break;
                }
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
        /// Creates a backup of the database
        /// </summary>
        /// <returns>A task that represents the asynchronous operation</returns>
        public virtual Task BackupDatabaseAsync(string fileName)
        {
            throw new DataException("This database provider does not support backup");
        }

        /// <summary>
        /// Inserts record into table. Returns inserted entity with identity
        /// </summary>
        /// <param name="entity"></param>
        /// <typeparam name="TEntity"></typeparam>
        /// <returns>Inserted entity</returns>
        public override TEntity InsertEntity<TEntity>(TEntity entity)
        {
            using var dataContext = CreateDataConnection();
            try
            {
                entity.Id = dataContext.InsertWithInt32Identity(entity);
            }
            // Ignore when we try insert foreign entity via InsertWithInt32IdentityAsync method
            catch (global::LinqToDB.SqlQuery.SqlException ex) when (ex.Message.StartsWith("Identity field must be defined for"))
            {
                dataContext.Insert(entity);
            }

            return entity;
        }

        /// <summary>
        /// Inserts record into table. Returns inserted entity with identity
        /// </summary>
        /// <param name="entity"></param>
        /// <typeparam name="TEntity"></typeparam>
        /// <returns>
        /// A task that represents the asynchronous operation
        /// The task result contains the inserted entity
        /// </returns>
        public override async Task<TEntity> InsertEntityAsync<TEntity>(TEntity entity)
        {
            using var dataContext = await CreateDataConnectionAsync();
            try
            {
                entity.Id = await dataContext.InsertWithInt32IdentityAsync(entity);
            }
            // Ignore when we try insert foreign entity via InsertWithInt32IdentityAsync method
            catch (global::LinqToDB.SqlQuery.SqlException ex) when (ex.Message.StartsWith("Identity field must be defined for"))
            {
                await dataContext.InsertAsync(entity);
            }

            return entity;
        }

        /// <summary>
        /// Restores the database from a backup
        /// </summary>
        /// <param name="backupFileName">The name of the backup file</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        public virtual Task RestoreDatabaseAsync(string backupFileName)
        {
            throw new DataException("This database provider does not support backup");
        }

        /// <summary>
        /// Re-index database tables
        /// </summary>
        /// <returns>A task that represents the asynchronous operation</returns>
        public virtual async Task ReIndexTablesAsync()
        {
            using var currentConnection = await CreateDataConnectionAsync();
            await currentConnection.ExecuteAsync($"REINDEX DATABASE \"{currentConnection.Connection.Database}\";");
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

        #endregion

        #region Properties

        protected override IDataProvider LinqToDbDataProvider => _dataProvider.Value;

        public int SupportedLengthOfBinaryHash => 0;

        public bool BackupSupported => false;

        #endregion
    }
}
