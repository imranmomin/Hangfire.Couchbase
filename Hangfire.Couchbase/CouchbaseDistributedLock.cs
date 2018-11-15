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
        private readonly string resource;

        public CouchbaseDistributedLock(string resource, TimeSpan timeout, CouchbaseStorage storage)
        {
            this.resource = resource;
            bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket);
            Acquire(timeout);
        }

        public void Dispose()
        {
            if (!string.IsNullOrEmpty(resourceId))
            {
                bucket.Remove(resourceId);
                resourceId = string.Empty;
                logger.Trace($"Lock released for {resource}");
            }

            bucket?.Dispose();
        }

        private void Acquire(TimeSpan timeout)
        {
            logger.Trace($"Trying to acquire lock for {resource} within {timeout.TotalSeconds} seconds");

            System.Diagnostics.Stopwatch acquireStart = new System.Diagnostics.Stopwatch();
            acquireStart.Start();

            BucketContext context = new BucketContext(bucket);

            while (string.IsNullOrEmpty(resourceId))
            {
                int expireOn = DateTime.UtcNow.ToEpoch();
                bool exists = context.Query<Lock>().Any(l => l.DocumentType == DocumentTypes.Lock && l.Name == resource && l.ExpireOn.HasValue && l.ExpireOn > expireOn);
                if (exists == false)
                {
                    Lock @lock = new Lock { Name = resource, ExpireOn = DateTime.UtcNow.Add(timeout).ToEpoch() };
                    IOperationResult<Lock> result = bucket.Insert(@lock.Id, @lock);

                    logger.Trace($"Acquired lock for {resource} in {acquireStart.Elapsed.TotalSeconds} seconds");
                    if (result.Success) resourceId = result.Id;
                }

                // check the timeout
                if (acquireStart.ElapsedMilliseconds > timeout.TotalMilliseconds)
                {
                    throw new CouchbaseDistributedLockException($"Could not place a lock on the resource '{resource}': Lock timeout.");
                }

                // sleep for 2000 millisecond
                logger.Trace($"Unable to acquire lock for {resource}. With wait for 2 seconds and retry");
                System.Threading.Thread.Sleep(2000);
            }
        }
    }
}