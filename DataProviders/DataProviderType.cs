using System.Runtime.Serialization;

namespace Q1.RedisSyncDataFromDB.Service.DataProviders
{
    /// <summary>
    /// Represents data provider type enumeration
    /// </summary>
    public enum DataProviderType
    {
        /// <summary>
        /// Unknown
        /// </summary>
        [EnumMember(Value = "")]
        Unknown,

        /// <summary>
        /// MS SQL Server
        /// </summary>
        [EnumMember(Value = "sqlserver")]
        SqlServer,

        /// <summary>
        /// MySQL
        /// </summary>
        [EnumMember(Value = "mysql")]
        MySql,

        /// <summary>
        /// PostgreSQL
        /// </summary>
        [EnumMember(Value = "postgresql")]
        PostgreSQL,

        /// <summary>
        /// MongoDB
        /// </summary>
        [EnumMember(Value = "mongo")]
        Mongo
    }
}