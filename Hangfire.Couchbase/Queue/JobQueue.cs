using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Couchbase;
using Couchbase.Core;
using Couchbase.Linq;

using Hangfire.Storage;

namespace Hangfire.Couchbase.Queue
{
    internal class JobQueue : IPersistentJobQueue
    {
        private readonly CouchbaseStorage storage;
        private const string DISTRIBUTED_LOCK_KEY = "locks:job:dequeue";
        private readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(1);
        private readonly TimeSpan checkInterval;
        private readonly object syncLock = new object();

        public JobQueue(CouchbaseStorage storage)
        {
            this.storage = storage;
            checkInterval = storage.Options.QueuePollInterval;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            int index = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                lock (syncLock)
                {
                    using (new CouchbaseDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
                    {
                        string queue = queues.ElementAt(index);
                        using (IBucket bucket = storage.Client.OpenBucket(storage.Options.Bucket))
                        {
                            BucketContext context = new BucketContext(bucket);

                            Documents.Queue data = context.Query<Documents.Queue>()
                                .Where(q => q.DocumentType == Documents.DocumentTypes.Queue && q.Name == queue)
                                .AsEnumerable()
                                .FirstOrDefault();

                            if (data != null)
                            {
                                Task<IOperationResult> task = bucket.RemoveAsync(data.Id);
                                task.Wait(cancellationToken);
                                if (task.Result.Success) return new FetchedJob(storage, data);
                            }
                        }
                    }
                }

                Thread.Sleep(checkInterval);
                index = (index + 1) % queues.Length;
            }
        }

        public void Enqueue(string queue, string jobId)
        {
            Documents.Queue data = new Documents.Queue
            {
                Name = queue,
                JobId = jobId
            };

            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.Bucket))
            {
                bucket.Insert(data.Id, data);
            }
        }
    }
}