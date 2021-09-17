namespace Q1.RedisSyncDataFromDB.Service.DataProviders
{
    /// <summary>
    /// Represents a connection string info
    /// </summary>
    public interface ICustomConnectionInfo
    {
        /// <summary>
        /// 数据库Key--与redis key对应关系
        /// </summary>
        string DBConnectionKey { get; set; }

        /// <summary>
        /// 链接字符串
        /// </summary>
        string DBConnectionString { get; set; }

        /// <summary>
        /// 数据库类型
        /// </summary>
        int DataProviderType { get; set; }

        /// <summary>
        /// 链接超时
        /// </summary>
        int Timeout { get; set; }

        /// <summary>
        /// 数据库
        /// </summary>
        string DataBase { get; set; } 
    }
}
