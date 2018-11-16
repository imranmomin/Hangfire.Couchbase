using System;

using Couchbase;
using Couchbase.Core;

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

            string id = $"{resource}:{DocumentTypes.Lock}".GenerateHash();

            while (string.IsNullOrEmpty(resourceId))
            {
                // default ttl for lock document
                TimeSpan ttl = DateTime.UtcNow.Add(timeout).AddMinutes(1).TimeOfDay;

                // read the document
                IDocumentResult<Lock> document = bucket.GetDocument<Lock>(id);
                
                // false means the document does not exists got ahead and create
                if (document.Success == false)
                {
                    Lock @lock = new Lock
                    {
                        Id = id,
                        Name = resource,
                        ExpireOn = DateTime.UtcNow.Add(timeout).ToEpoch()
                    };

                    IOperationResult<Lock> result = bucket.Insert(@lock.Id, @lock, ttl);
                    if (result.Success) { resourceId = id; }
                }
                else
                {
                    if (document.Content.ExpireOn < DateTime.UtcNow.ToEpoch())
                    {
                        IDocumentFragment<Lock> result = bucket.MutateIn<Lock>(id)
                            .WithCas(document.Document.Cas)
                            .WithExpiry(ttl)
                            .Upsert(l => l.ExpireOn, DateTime.UtcNow.ToEpoch(), false)
                            .Execute();

                        if (result.Success) resourceId = id;
                    }
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

            logger.Trace($"Acquired lock for {resource} in {acquireStart.Elapsed.TotalSeconds} seconds");
        }
    }
}