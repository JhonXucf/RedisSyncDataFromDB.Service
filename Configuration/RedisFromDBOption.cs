using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Q1.RedisSyncDataFromDB.Service.Configuration
{
    public class RedisFromDBOptions
    {
        public List<RedisAndDBRelation> RedisAndDBRelations { get; set; }
        public class RedisAndDBRelation
        {
            public string RedisChannelKey { get; set; }
            public string DBConnectionKey { get; set; }
        }
    }
}
