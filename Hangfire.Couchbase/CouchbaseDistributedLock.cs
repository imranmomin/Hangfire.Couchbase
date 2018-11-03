using System;
using System.Linq;

using Couchbase;
using Couchbase.Core;
using Couchbase.Linq;

using Hangfire.Logging;
using Hangfire.Couchbase.Helper;
using Hangfire.Couchbase.Documents;

namespace Hangfire.Couchbase
{
    internal class CouchbaseDistributedLock : IDisposable
    {
        private readonly ILog logger = LogProvider.For<CouchbaseDistributedLock>();
        private readonly IBucket bucket;
        private string resourceId;

        public CouchbaseDistributedLock(string resource, TimeSpan timeout, CouchbaseStorage storage)
        {
            bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket);
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
            logger.Trace($"Trying to acquire lock for {name} within {timeout.TotalSeconds} seconds");

            System.Diagnostics.Stopwatch acquireStart = new System.Diagnostics.Stopwatch();
            acquireStart.Start();

            BucketContext context = new BucketContext(bucket);

            while (string.IsNullOrEmpty(resourceId))
            {
                int expireOn = DateTime.UtcNow.ToEpoch();
                bool exists = context.Query<Lock>().Any(l => l.DocumentType == DocumentTypes.Lock && l.Name == name && l.ExpireOn.HasValue && l.ExpireOn > expireOn);
                if (exists == false)
                {
                    Lock @lock = new Lock { Name = name, ExpireOn = DateTime.UtcNow.Add(timeout).ToEpoch() };
                    IOperationResult<Lock> result = bucket.Insert(@lock.Id, @lock);
                    if (result.Success) resourceId = result.Id;
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