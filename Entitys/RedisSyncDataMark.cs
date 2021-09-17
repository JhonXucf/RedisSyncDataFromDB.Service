using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Q1.RedisSyncDataFromDB.Service.Entitys
{
    public class RedisSyncDataMark : BaseEntity
    {
        /// <summary>
        /// redis订阅的key
        /// </summary>
        public string RedisChannelKey { get; set; }

        /// <summary>
        /// redis要发送的消息
        /// </summary>
        public string RedisJsonMessage { get; set; }

        /// <summary>
        /// 时间戳--进行数据匹配
        /// </summary>
        public long TimeStamp { get; set; }
        public override string ToString()
        {
            return string.Format("RedisChannelKey : {0} RedisJsonMessage : {1} TimeStamp : {2} Datetime : {3}", RedisChannelKey, RedisJsonMessage, TimeStamp, DateTime.Now);
        }
    }
}
