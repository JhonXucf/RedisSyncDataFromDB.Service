using System;
using System.Threading.Tasks;

namespace Q1.RedisSyncDataFromDB.Service
{
    public interface IRedisSyncDataService : IDisposable
    {
        ValueTask StartAsync();
        ValueTask StopAsync();
    }
}
