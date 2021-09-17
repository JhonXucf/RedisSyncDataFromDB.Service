using LinqToDB;
using LinqToDB.Common;
using LinqToDB.Data;
using LinqToDB.DataProvider;
using LinqToDB.Mapping;
using LinqToDB.SchemaProvider;
using LinqToDB.SqlProvider;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Q1.RedisSyncDataFromDB.Service.DataProviders.MongoService
{
    public class MongoDbService : IMongoDbService, IDataProvider
    {
        private MongoClientSettings _MongoClientSettings = null;
        private IMongoDatabase _mongoDatabase = null;
        private IClientSessionHandle _clientSession = null;

        private readonly MongoUrl _mongoUrl;

        private string _mongoConfig;
        private string _databaseName;

        public MongoDbService(
             string mongoConfig
            , string databaseName)
        {
            _mongoConfig = mongoConfig;
            _databaseName = databaseName;
            _mongoUrl = new MongoUrl(_mongoConfig);
        }

        public IMongoDatabase MongoDatabase
        {
            get
            {
                MongoClient client = new MongoClient(_mongoUrl);
                _clientSession = client.StartSession();
                if (client != null)
                {
                    _mongoDatabase = client.GetDatabase(_databaseName);
                }
                return _mongoDatabase;
            }
        }

        public IMongoDatabase MongoClusterDatabase
        {
            get
            {
                MongoClient client = new MongoClient(_MongoClientSettings);
                if (client != null)
                {
                    _mongoDatabase = client.GetDatabase(_databaseName);
                }
                return _mongoDatabase;
            }
        }

        public virtual IMongoCollection<TDocument> GetCollection<TDocument>(string name, MongoCollectionSettings settings = null)
        {
            return MongoDatabase.GetCollection<TDocument>(name, settings);
        }

        /// <summary>
        /// 开始事务
        /// </summary>
        public void StartTransaction()
        {
            _clientSession.StartTransaction(new TransactionOptions(
                readConcern: ReadConcern.Snapshot,
                writeConcern: WriteConcern.WMajority));
        }

        /// <summary>
        /// 终止事务并回滚
        /// </summary>
        public void AbortTransaction()
        {
            _clientSession.AbortTransaction();
        }

        /// <summary>
        /// 提交事务
        /// </summary>
        public void CommitTransaction()
        {
            _clientSession.CommitTransaction();
        }

        public string Name => throw new NotImplementedException();

        public string ConnectionNamespace => throw new NotImplementedException();

        public Type DataReaderType => throw new NotImplementedException();

        public MappingSchema MappingSchema => throw new NotImplementedException();

        public SqlProviderFlags SqlProviderFlags => throw new NotImplementedException();

        public TableOptions SupportedTableOptions => throw new NotImplementedException();
        public IDbConnection CreateConnection(string connectionInfo)
        {
            throw new NotImplementedException();
        }

        public ISqlBuilder CreateSqlBuilder(MappingSchema mappingSchema)
        {
            throw new NotImplementedException();
        }

        public ISqlOptimizer GetSqlOptimizer()
        {
            throw new NotImplementedException();
        }

        public void InitCommand(DataConnection dataConnection, CommandType commandType, string commandText, DataParameter[] parameters, bool withParameters)
        {
            throw new NotImplementedException();
        }

        public void DisposeCommand(DataConnection dataConnection)
        {
            throw new NotImplementedException();
        }

        public object GetConnectionInfo(DataConnection dataConnection, string parameterName)
        {
            throw new NotImplementedException();
        }

        public Expression GetReaderExpression(IDataReader reader, int idx, Expression readerExpression, Type toType)
        {
            throw new NotImplementedException();
        }

        public bool? IsDBNullAllowed(IDataReader reader, int idx)
        {
            throw new NotImplementedException();
        }

        public void SetParameter(DataConnection dataConnection, IDbDataParameter parameter, string name, DbDataType dataType, object value)
        {
            throw new NotImplementedException();
        }

        public Type ConvertParameterType(Type type, DbDataType dataType)
        {
            throw new NotImplementedException();
        }

        public CommandBehavior GetCommandBehavior(CommandBehavior commandBehavior)
        {
            throw new NotImplementedException();
        }

        public IDisposable ExecuteScope(DataConnection dataConnection)
        {
            throw new NotImplementedException();
        }

        public ISchemaProvider GetSchemaProvider()
        {
            throw new NotImplementedException();
        }

        public BulkCopyRowsCopied BulkCopy<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source)
        {
            throw new NotImplementedException();
        }

        public Task<BulkCopyRowsCopied> BulkCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<BulkCopyRowsCopied> BulkCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IAsyncEnumerable<T> source, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        // var client = new MongoClient("mongodb://localhost:27017,localhost:27018,localhost:27019");
    }
}
