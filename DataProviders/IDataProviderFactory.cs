using Q1.RedisSyncDataFromDB.Service.Configuration;
using System;

namespace Q1.RedisSyncDataFromDB.Service.DataProviders
{
    public interface IDataProviderFactory : IDisposable
    {
        ICustomDataProvider CreateProvider(ICustomConnectionInfo dBConnectionInfo);
        ICustomDataProvider GetOrCreateProvider(ICustomConnectionInfo dBConnectionInfo);
    }
}
