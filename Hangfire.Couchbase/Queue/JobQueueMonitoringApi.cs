using System.Linq;
using System.Collections.Generic;

using Couchbase.Core;
using Couchbase.Linq;
using Hangfire.Couchbase.Documents;

namespace Hangfire.Couchbase.Queue
{
    internal class JobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly CouchbaseStorage storage;
        private readonly IEnumerable<string> queues;

        public JobQueueMonitoringApi(CouchbaseStorage storage)
        {
            this.storage = storage;
            queues = storage.Options.Queues;
        }

        public IEnumerable<string> GetQueues() => queues;

        public int GetEnqueuedCount(string queue)
        {
            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.Bucket))
            {
                BucketContext context = new BucketContext(bucket);
                return context.Query<Documents.Queue>().Count(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue);
            }
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.Bucket))
            {
                BucketContext context = new BucketContext(bucket);
                return context.Query<Documents.Queue>()
                    .Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue)
                    .Select(c => c.JobId)
                    .AsEnumerable()
                    .Skip(from).Take(perPage)
                    .ToList();
            }
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage) => GetEnqueuedJobIds(queue, from, perPage);

    }
}