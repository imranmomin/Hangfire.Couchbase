using System;
using System.Linq;
using System.Threading;

using Couchbase;
using Couchbase.Core;
using Couchbase.Linq;
using Hangfire.Storage;
using Hangfire.Logging;

using Hangfire.Couchbase.Helper;
using Hangfire.Couchbase.Documents;

namespace Hangfire.Couchbase.Queue
{
    internal class JobQueue : IPersistentJobQueue
    {
        private readonly ILog logger = LogProvider.For<JobQueue>();
        private readonly CouchbaseStorage storage;
        private const string DISTRIBUTED_LOCK_KEY = "locks:job:dequeue:{0}";
        private readonly TimeSpan defaultLockTimeout;
        private readonly TimeSpan invisibilityTimeout = TimeSpan.FromMinutes(30);
        private readonly object syncLock = new object();

        public JobQueue(CouchbaseStorage storage)
        {
            this.storage = storage;
            defaultLockTimeout = TimeSpan.FromSeconds(15) + storage.Options.QueuePollInterval;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            int index = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                lock (syncLock)
                {
                    int invisibilityTimeoutEpoch = DateTime.UtcNow.Add(invisibilityTimeout.Negate()).ToEpoch();
                    string queue = queues.ElementAt(index);
                    string lockName = string.Format(DISTRIBUTED_LOCK_KEY, queue)
                        .Replace("  ", "-")
                        .Replace(" ", "-")
                        .ToLower();

                    logger.Trace($"Looking for any jobs under '{queue}' queue");

                    using (new CouchbaseDistributedLock(lockName, defaultLockTimeout, storage))
                    {
                        using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
                        {
                            BucketContext context = new BucketContext(bucket);
                            Documents.Queue data = context.Query<Documents.Queue>()
                                .Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue && (N1QlFunctions.IsMissing(q.FetchedAt) || q.FetchedAt < invisibilityTimeoutEpoch))
                                .OrderBy(q => q.CreatedOn)
                                .FirstOrDefault();

                            if (data != null)
                            {
                                IDocumentFragment<Documents.Queue> result = bucket.MutateIn<Documents.Queue>(data.Id)
                                    .Upsert(q => q.FetchedAt, DateTime.UtcNow.ToEpoch(), true)
                                    .Execute();

                                if (result.Success) return new FetchedJob(storage, data);
                            }
                        }
                    }

                    logger.Trace($"Unable to find any jobs under '{queue}' queue");
                }

                Thread.Sleep(storage.Options.QueuePollInterval);
                index = (index + 1) % queues.Length;
            }
        }

        public void Enqueue(string queue, string jobId)
        {
            Documents.Queue data = new Documents.Queue
            {
                Name = queue,
                JobId = jobId,
                CreatedOn = DateTime.UtcNow
            };

            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                bucket.Insert(data.Id, data);
            }
        }
    }
}