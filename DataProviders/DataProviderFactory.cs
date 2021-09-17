using Q1.RedisSyncDataFromDB.Service.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Q1.RedisSyncDataFromDB.Service.DataProviders
{
    public class DataProviderFactory : IDataProviderFactory
    {
        #region Filed

        private readonly Dictionary<string, ICustomDataProvider> _customDataProviders = new Dictionary<string, ICustomDataProvider>();

        #endregion

        /// <summary>
        /// 创建dataprovider
        /// </summary>
        /// <param name="dBConnectionInfo"></param>
        /// <returns>ICustomDataProvider</returns>
        public ICustomDataProvider CreateProvider(ICustomConnectionInfo dBConnectionInfo)
        {
            if (dBConnectionInfo == null)
            {
                throw new ArgumentNullException(nameof(dBConnectionInfo));
            }
            ICustomDataProvider dataProvider = null;
            switch ((DataProviderType)dBConnectionInfo.DataProviderType)
            {
                case DataProviderType.Unknown:
                case DataProviderType.SqlServer:
                    dataProvider = new MsSqlCustomDataProvider(dBConnectionInfo);
                    break;
                case DataProviderType.MySql:
                    dataProvider = new MySqlCustomDataProvider(dBConnectionInfo);
                    break;
                case DataProviderType.PostgreSQL:
                    dataProvider = new PostgreSqlDataProvider(dBConnectionInfo);
                    break;
                case DataProviderType.Mongo:
                    dataProvider = new MongoDataProvider(dBConnectionInfo);
                    break;
                default:
                    break;
            }
            _customDataProviders[dBConnectionInfo.DBConnectionKey] = dataProvider;
            return dataProvider;
        }

        /// <summary>
        /// 获取或创建dataprovider
        /// </summary>
        /// <param name="dBConnectionInfo"></param>
        /// <returns>ICustomDataProvider</returns>
        public ICustomDataProvider GetOrCreateProvider(ICustomConnectionInfo dBConnectionInfo)
        {
            if (dBConnectionInfo == null)
            {
                throw new ArgumentNullException(nameof(dBConnectionInfo));
            }
            if (_customDataProviders.ContainsKey(dBConnectionInfo.DBConnectionKey))
            {
                return _customDataProviders[dBConnectionInfo.DBConnectionKey];
            }
            return CreateProvider(dBConnectionInfo);
        }

        public void Dispose()
        {
            _customDataProviders.Clear();
        }
    }
}
