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
        private const string DISTRIBUTED_LOCK_KEY = "locks:job:dequeue";
        private readonly TimeSpan defaultLockTimeout;
        private readonly TimeSpan invisibilityTimeout = TimeSpan.FromMinutes(15);
        private static readonly object syncLock = new object();

        public JobQueue(CouchbaseStorage storage)
        {
            this.storage = storage;
            defaultLockTimeout = TimeSpan.FromSeconds(15) + storage.Options.QueuePollInterval;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            lock (syncLock)
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    int invisibilityTimeoutEpoch = DateTime.UtcNow.Add(invisibilityTimeout.Negate()).ToEpoch();
                    logger.Trace("Looking for any jobs from the queue");

                    using (new CouchbaseDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
                    {
                        using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
                        {
                            BucketContext context = new BucketContext(bucket);
                            Documents.Queue data = context.Query<Documents.Queue>()
                                .Where(q => q.DocumentType == DocumentTypes.Queue && queues.Contains(q.Name) && (N1QlFunctions.IsMissing(q.FetchedAt) || q.FetchedAt < invisibilityTimeoutEpoch))
                                .OrderBy(q => q.CreatedOn)
                                .FirstOrDefault();

                            if (data != null)
                            {
                                IDocumentFragment<Documents.Queue> result = bucket.MutateIn<Documents.Queue>(data.Id)
                                    .Upsert(q => q.FetchedAt, DateTime.UtcNow.ToEpoch(), true)
                                    .Execute();

                                if (result.Success)
                                {
                                    logger.Trace($"Found job {data.JobId} from the queue {data.Name}");
                                    return new FetchedJob(storage, data);
                                }
                            }
                        }
                    }

                    logger.Trace($"Unable to find any jobs in the queue. Will check the queue for jobs in {storage.Options.QueuePollInterval.TotalSeconds} seconds");
                    cancellationToken.WaitHandle.WaitOne(storage.Options.QueuePollInterval);
                }
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