namespace Hangfire.Couchbase.RabbitMq
{
    public static class RabbitMqCouchbaseStorageExtension
    {
        public static CouchbaseStorage UseRabbitMq(this CouchbaseStorage storage, params string[] queues)
        {
            return storage;
        }
    }
}