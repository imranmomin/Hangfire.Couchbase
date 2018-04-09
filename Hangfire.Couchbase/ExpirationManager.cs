using System;
using System.Linq;
using System.Threading;

using Couchbase.Core;
using Couchbase.Linq;
using Hangfire.Server;
using Hangfire.Logging;

using Hangfire.Couchbase.Helper;
using Hangfire.Couchbase.Documents;

namespace Hangfire.Couchbase
{
#pragma warning disable 618
    internal class ExpirationManager : IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog logger = LogProvider.For<ExpirationManager>();
        private const string DISTRIBUTED_LOCK_KEY = "expirationmanager";
        private static readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(5);
        private static readonly DocumentTypes[] documents = { DocumentTypes.Lock, DocumentTypes.Job, DocumentTypes.List, DocumentTypes.Set, DocumentTypes.Hash, DocumentTypes.Counter };
        private readonly TimeSpan checkInterval;
        private readonly CouchbaseStorage storage;

        public ExpirationManager(CouchbaseStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            checkInterval = storage.Options.ExpirationCheckInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.Bucket))
            {
                BucketContext context = new BucketContext(bucket);
                foreach (DocumentTypes type in documents)
                {
                    logger.Debug($"Removing outdated records from the '{type}' document.");

                    using (new CouchbaseDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
                    {
                        string[] ids = context.Query<DocumentBase>()
                            .Where(d => d.DocumentType == type && d.ExpireOn.HasValue && d.ExpireOn < DateTime.UtcNow.ToEpoch())
                            .Select(d => d.Id)
                            .ToArray();
                        
                        Array.ForEach(ids, id => bucket.RemoveAsync(id).Wait(cancellationToken));

                        logger.Trace($"Outdated records removed {ids.Length} records from the '{type}' document.");
                    }

                    cancellationToken.WaitHandle.WaitOne(checkInterval);
                }
            }
        }
    }
}