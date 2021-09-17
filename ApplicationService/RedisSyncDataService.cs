using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Q1.RedisSyncDataFromDB.Service.Configuration;
using Q1.RedisSyncDataFromDB.Service.DataProviders;
using Q1.RedisSyncDataFromDB.Service.Entitys;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Q1.RedisSyncDataFromDB.Service
{
    public class RedisSyncDataService : IRedisSyncDataService
    {
        #region 属性

        /// <summary>
        /// 日志记录器
        /// </summary>
        private readonly ILogger<RedisSyncDataService> _logger;

        /// <summary>
        /// REDIS连接
        /// </summary>
        private readonly ConnectionMultiplexer _multiplexer;

        /// <summary>
        /// 配置
        /// </summary>
        IConfiguration _configuration;

        /// <summary>
        /// Redis database
        /// </summary>
        IDatabase _database;

        /// <summary>
        /// 数据访问提供
        /// </summary>
        IDataProviderFactory _dataProviderFactory;

        /// <summary>
        /// redis和数据库映射关系
        /// </summary>
        private RedisFromDBOptions _redisFromDBOptions;

        /// <summary>
        /// 数据库链接信息
        /// </summary>
        private DBOptions _dBOptions;
        #endregion

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="logger">日志记录器</param> 
        /// <param name="multiplexer">Redis连接</param>
        /// <param name="RedisSyncDataService">同步服务</param>
        public RedisSyncDataService(ILogger<RedisSyncDataService> logger, IConfiguration configuration, ConnectionMultiplexer multiplexer, IDataProviderFactory dataProviderFactory, IOptionsMonitor<List<RedisFromDBOptions.RedisAndDBRelation>> redisFromDBOptions, IOptionsMonitor<List<DBConnectionInfo>> dBOptions)
        {
            _logger = logger;
            _configuration = configuration;
            _multiplexer = multiplexer;

            _database = _multiplexer.GetDatabase();

            _dataProviderFactory = dataProviderFactory;

            //获取配置文件值
            _redisFromDBOptions = new RedisFromDBOptions();
            _redisFromDBOptions.RedisAndDBRelations = redisFromDBOptions.CurrentValue;

            _dBOptions = new DBOptions();
            _dBOptions.DBConnectionInfos = dBOptions.CurrentValue;

            redisFromDBOptions.OnChange(p => _redisFromDBOptions.RedisAndDBRelations = p);

            dBOptions.OnChange(p => _dBOptions.DBConnectionInfos = p);
        }

        /// <summary>
        /// 开始同步任务
        /// </summary>
        /// <returns></returns>
        public async ValueTask StartAsync()
        {
            if (_redisFromDBOptions == null || !_redisFromDBOptions.RedisAndDBRelations.Any())
            {
                return;
            }

            //获取订阅者，没有订阅返回
            var sub = _multiplexer.GetSubscriber();
            if (sub == null)
            {
                return;
            }

            foreach (var relation in _redisFromDBOptions.RedisAndDBRelations)
            {
                //对应关系找到数据库
                var dBConnectionInfo = _dBOptions.DBConnectionInfos.Find(para => para.DBConnectionKey.Equals(relation.DBConnectionKey));

                if (dBConnectionInfo == null)
                {
                    continue;
                }

                long timeStamp = 0;
                //redis是否有缓存 redis key对应的时间戳
                if (_database.StringGet(relation.RedisChannelKey).HasValue)
                {
                    timeStamp = (long)_database.StringGet(relation.RedisChannelKey);
                }

                //根据连接信息创建数据提供者
                var dataProvider = _dataProviderFactory.GetOrCreateProvider(dBConnectionInfo);

                //获取数据
                IList<RedisSyncDataMark> marks = await GetDataFromDB(dataProvider, dBConnectionInfo.DataProviderType, relation.RedisChannelKey, timeStamp);

                if (marks == null || marks.Count == 0)
                {
                    continue;
                }

                foreach (var mark in marks.OrderBy(p => p.TimeStamp))
                {
                    timeStamp = mark.TimeStamp;

                    _logger.LogInformation(mark.ToString());

                    //数据发给订阅者
                    await PublishAsync(sub, mark);
                }

                //将最新时间戳缓存到redis
                await _database.StringSetAsync(relation.RedisChannelKey, timeStamp);
            }
        }

        /// <summary>
        /// 获取数据库数据
        /// </summary>
        /// <param name="provider">数据提供者</param> 
        /// <param name="dbType">数据库类型</param>
        /// <param name="redisChannelKey">缓存的Redis key</param>
        /// <param name="timeStamp">时间戳</param>
        /// <returns></returns>
        private async ValueTask<IList<RedisSyncDataMark>> GetDataFromDB(ICustomDataProvider provider, int dbType, string redisChannelKey, long timeStamp)
        {
            //mongo的实现方式不一样，单独请求
            if (dbType == (int)DataProviderType.Mongo)
            {
                return await provider.QueryAsync<RedisSyncDataMark>(nameof(RedisSyncDataMark), query => query.TimeStamp > timeStamp, null);
            }
            return await provider.QueryAsync<RedisSyncDataMark>(string.Format("select * from " + nameof(RedisSyncDataMark) + " where RedisChannelKey = '{0}' and TimeStamp > {1}", redisChannelKey, timeStamp), null);
        }

        /// <summary>
        /// 发布到redis订阅
        /// </summary>
        /// <param name="sub">订阅者</param>
        /// <param name="mark">数据</param>
        /// <returns></returns>
        private async ValueTask PublishAsync(ISubscriber sub, RedisSyncDataMark mark)
        {
            await sub.PublishAsync(mark.RedisChannelKey, mark.RedisJsonMessage);
        }

        public async ValueTask StopAsync()
        {
            Dispose();
            await Task.CompletedTask;
        }
        public void Dispose()
        {
            _dataProviderFactory.Dispose();
        }
    }
}
