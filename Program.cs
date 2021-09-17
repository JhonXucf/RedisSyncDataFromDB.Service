using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Q1.RedisSyncDataFromDB.Service.DataProviders;
using System.Collections.Generic;

namespace Q1.RedisSyncDataFromDB.Service
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService<RedisSyncDataWorker>();
                    services.AddLogging(opt => opt.AddConsole(c =>
                                         {
                                             c.TimestampFormat = "[yyyy-MM-dd HH:mm:ss]";
                                         }));
                    services.AddRedisMultiplexer(hostContext.Configuration);

                    services.AddSingleton<IRedisSyncDataService, RedisSyncDataService>();

                    services.AddSingleton<IDataProviderFactory, DataProviderFactory>();

                    services.Configure<List<Configuration.RedisFromDBOptions.RedisAndDBRelation>>(hostContext.Configuration.GetSection(nameof(Configuration.RedisFromDBOptions)));

                    services.Configure<List<Configuration.DBConnectionInfo>>(hostContext.Configuration.GetSection(nameof(Configuration.DBOptions)));
                });
    }
}
