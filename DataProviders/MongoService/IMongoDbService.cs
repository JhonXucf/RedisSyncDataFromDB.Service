using MongoDB.Driver;

namespace Q1.RedisSyncDataFromDB.Service.DataProviders.MongoService
{
    public interface IMongoDbService
    {
        IMongoCollection<TDocument> GetCollection<TDocument>(string name, MongoCollectionSettings settings = null);

    }
}
