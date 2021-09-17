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
        #region ����

        /// <summary>
        /// ��־��¼��
        /// </summary>
        private readonly ILogger<RedisSyncDataWorker> _logger;

        /// <summary>
        /// ͬ������
        /// </summary>
        private IRedisSyncDataService _RedisSyncDataService;

        /// <summary>
        /// ����
        /// </summary>
        private IConfiguration _configuration;

        /// <summary>
        /// ͬ�����
        /// </summary>
        public int _syncDataInterval { get; set; } = 1000;

        #endregion

        /// <summary>
        /// ���캯��
        /// </summary>
        /// <param name="logger">��־��¼��</param>  
        /// <param name="RedisSyncDataService">ͬ������</param>
        public RedisSyncDataWorker(ILogger<RedisSyncDataWorker> logger, IConfiguration configuration, IRedisSyncDataService RedisSyncDataService)
        {
            _logger = logger;
            _RedisSyncDataService = RedisSyncDataService;
            _configuration = configuration;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            string interval = _configuration["SyncDataInterval"];
            //��������õĻ��滻
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
                //��ʼͬ������
                await _RedisSyncDataService.StartAsync( );

                //��ʱ
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
