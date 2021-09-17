using Q1.RedisSyncDataFromDB.Service.DataProviders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Q1.RedisSyncDataFromDB.Service.Configuration
{
    public class DBOptions
    {
        public List<DBConnectionInfo> DBConnectionInfos { get; set; }

    }
    public class DBConnectionInfo : ICustomConnectionInfo
    {
        public string DBConnectionKey { get; set; }
        public string DBConnectionString { get; set; }
        public int DataProviderType { get; set; }
        public int Timeout { get; set; }
        public string DataBase { get; set; }

    }
}
