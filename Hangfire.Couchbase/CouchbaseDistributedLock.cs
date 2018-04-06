using System;
using System.Linq;
using System.Threading.Tasks;

using Couchbase;
using Couchbase.Core;
using Couchbase.Linq;

using Hangfire.Couchbase.Documents;

namespace Hangfire.Couchbase
{
    internal class CouchbaseDistributedLock : IDisposable
    {
        private readonly IBucket bucket;
        private string resourceId;

        public CouchbaseDistributedLock(string resource, TimeSpan timeout, CouchbaseStorage storage)
        {
            bucket = storage.Client.OpenBucket(storage.Options.Bucket);
            Acquire(resource, timeout);
        }

        public void Dispose()
        {
            if (!string.IsNullOrEmpty(resourceId))
            {
                bucket.Remove(resourceId);
                resourceId = string.Empty;
            }

            bucket?.Dispose();
        }

        private void Acquire(string name, TimeSpan timeout)
        {
            System.Diagnostics.Stopwatch acquireStart = new System.Diagnostics.Stopwatch();
            acquireStart.Start();

            BucketContext context = new BucketContext(bucket);

            while (string.IsNullOrEmpty(resourceId))
            {
                bool exists = context.Query<Lock>().Any(l => l.DocumentType == DocumentTypes.Lock && l.Name == name);
                if (exists == false)
                {
                    Lock @lock = new Lock { Name = name, ExpireOn = DateTime.UtcNow.Add(timeout) };
                    Task<IOperationResult<Lock>> task = bucket.InsertAsync(@lock.Id, @lock);
                    Task continueTask = task.ContinueWith(t => resourceId = @lock.Id, TaskContinuationOptions.OnlyOnRanToCompletion);
                    continueTask.Wait();
                }

                // check the timeout
                if (acquireStart.ElapsedMilliseconds > timeout.TotalMilliseconds)
                {
                    throw new CouchbaseDistributedLockException($"Could not place a lock on the resource '{name}': Lock timeout.");
                }

                // sleep for 1000 millisecond
                System.Threading.Thread.Sleep(1000);
            }
        }
    }
}