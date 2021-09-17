using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Q1.RedisSyncDataFromDB.Service
{
    public class RedisSyncDataWorker : IHostedService, IDisposable
    {
        #region 属性

        /// <summary>
        /// 日志记录器
        /// </summary>
        private readonly ILogger<RedisSyncDataWorker> _logger;

        /// <summary>
        /// 同步服务
        /// </summary>
        private IRedisSyncDataService _RedisSyncDataService;

        /// <summary>
        /// 配置
        /// </summary>
        private IConfiguration _configuration;

        /// <summary>
        /// 同步间隔
        /// </summary>
        public int _syncDataInterval { get; set; } = 1000;

        #endregion

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="logger">日志记录器</param>  
        /// <param name="RedisSyncDataService">同步服务</param>
        public RedisSyncDataWorker(ILogger<RedisSyncDataWorker> logger, IConfiguration configuration, IRedisSyncDataService RedisSyncDataService)
        {
            _logger = logger;
            _RedisSyncDataService = RedisSyncDataService;
            _configuration = configuration;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            string interval = _configuration["SyncDataInterval"];
            //如果有配置的话替换
            if (int.TryParse(interval, out int syncDataInterval))
            {
                _syncDataInterval = syncDataInterval;
            }
            _logger.LogInformation("RedisSyncDataWorker Start at: {time}", DateTimeOffset.Now);
            await ExecuteAsync(cancellationToken);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await StopExecuteAsync(cancellationToken);
        }

        private async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("RedisSyncDataWorker running at: {time}", DateTimeOffset.Now);
            while (!stoppingToken.IsCancellationRequested)
            {        
                //开始同步任务
                await _RedisSyncDataService.StartAsync( );

                //延时
                await Task.Delay(_syncDataInterval, stoppingToken);
            }
        }
        private async Task StopExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("RedisSyncDataWorker Stop at: {time}", DateTimeOffset.Now);
            await _RedisSyncDataService.StopAsync();
            Dispose();
        }

        public void Dispose()
        {
            _RedisSyncDataService.Dispose();
        }
    }
}
