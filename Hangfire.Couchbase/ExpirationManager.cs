using System;
using System.Linq;
using System.Threading;
using System.Collections.Generic;

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
        private static readonly TimeSpan defaultLockTimeout = TimeSpan.FromSeconds(15);
        private static readonly DocumentTypes[] documents = { DocumentTypes.Lock, DocumentTypes.Job, DocumentTypes.List, DocumentTypes.Set, DocumentTypes.Hash, DocumentTypes.Counter };
        private readonly CouchbaseStorage storage;

        public ExpirationManager(CouchbaseStorage storage) => this.storage = storage ?? throw new ArgumentNullException(nameof(storage));

        public void Execute(CancellationToken cancellationToken)
        {
            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                BucketContext context = new BucketContext(bucket);
                foreach (DocumentTypes type in documents)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    logger.Debug($"Removing outdated records from the '{type}' document.");

                    using (new CouchbaseDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
                    {
                        IList<string> ids = context.Query<DocumentBase>()
                            .Where(d => d.DocumentType == type && d.ExpireOn.HasValue && d.ExpireOn < DateTime.UtcNow.ToEpoch())
                            .Select(d => d.Id)
                            .ToList();

                        bucket.Remove(ids, TimeSpan.FromSeconds(30));
                        logger.Trace($"Outdated records removed {ids.Count} records from the '{type}' document.");
                    }

                    cancellationToken.WaitHandle.WaitOne(storage.Options.ExpirationCheckInterval);
                }
            }
        }
    }
}