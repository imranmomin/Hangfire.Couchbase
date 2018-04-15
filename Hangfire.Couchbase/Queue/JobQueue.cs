using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Couchbase;
using Couchbase.Core;
using Couchbase.Linq;
using Hangfire.Storage;
using Hangfire.Couchbase.Documents;

namespace Hangfire.Couchbase.Queue
{
    internal class JobQueue : IPersistentJobQueue
    {
        private readonly CouchbaseStorage storage;
        private const string DISTRIBUTED_LOCK_KEY = "locks:job:dequeue";
        private readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(1);
        private readonly object syncLock = new object();

        public JobQueue(CouchbaseStorage storage) => this.storage = storage;

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
                        using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
                        {
                            BucketContext context = new BucketContext(bucket);
                            Documents.Queue data = context.Query<Documents.Queue>()
                                .OrderBy(q => q.CreatedOn)
                                .FirstOrDefault(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue);

                            if (data != null)
                            {
                                Task<IOperationResult> task = bucket.RemoveAsync(data.Id);
                                task.Wait(cancellationToken);
                                if (task.Result.Success) return new FetchedJob(storage, data);
                            }
                        }
                    }
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