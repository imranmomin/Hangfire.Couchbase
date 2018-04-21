using System;
using Couchbase.Core;
using Hangfire.Storage;

// ReSharper disable once CheckNamespace
namespace Hangfire.Couchbase.Queue
{
    internal class FetchedJob : IFetchedJob
    {
        private readonly IBucket bucket;

        public FetchedJob(CouchbaseStorage storage, Documents.Queue data)
        {
            bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket);

            Id = data.Id;
            JobId = data.JobId;
            Queue = data.Name;
        }

        private string Id { get; }

        public string JobId { get; }

        private string Queue { get; }

        public void Dispose() => bucket.Dispose();

        public void RemoveFromQueue() => bucket.Remove(Id);

        public void Requeue()
        {
            Documents.Queue data = new Documents.Queue
            {
                Name = Queue,
                JobId = JobId,
                CreatedOn = DateTime.UtcNow
            };

            bucket.Insert(Id, data);
        }
    }
}
