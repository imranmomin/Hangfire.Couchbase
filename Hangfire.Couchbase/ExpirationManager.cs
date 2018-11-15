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
        private readonly ILog logger = LogProvider.For<ExpirationManager>();
        private const string DISTRIBUTED_LOCK_KEY = "locks:expirationmanager";
        private readonly TimeSpan defaultLockTimeout;
        private readonly DocumentTypes[] documents = { DocumentTypes.Lock, DocumentTypes.Job, DocumentTypes.List, DocumentTypes.Set, DocumentTypes.Hash, DocumentTypes.Counter };
        private readonly CouchbaseStorage storage;

        public ExpirationManager(CouchbaseStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            defaultLockTimeout = TimeSpan.FromSeconds(15) + storage.Options.ExpirationCheckInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                BucketContext context = new BucketContext(bucket);
                foreach (DocumentTypes type in documents)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    using (new CouchbaseDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
                    {
                        logger.Trace($"Removing outdated records from the '{type}' document.");

                        IQueryable<DocumentBase> query;

                        // remove only the aggregate counters when the type is Counter
                        if (type == DocumentTypes.Counter)
                        {
                            query = context.Query<Counter>().Where(d => d.DocumentType == type &&
                                                                        d.Type == CounterTypes.Aggregate &&
                                                                        d.ExpireOn.HasValue &&
                                                                        d.ExpireOn < DateTime.UtcNow.ToEpoch());
                        }
                        else
                        {
                            query = context.Query<Counter>().Where(d => d.DocumentType == type &&
                                                                        d.ExpireOn.HasValue &&
                                                                        d.ExpireOn < DateTime.UtcNow.ToEpoch());
                        }

                        IList<string> ids = query.Select(d => d.Id).ToList();
                        bucket.Remove(ids, TimeSpan.FromSeconds(30));
                        logger.Trace($"Outdated records removed {ids.Count} records from the '{type}' document.");
                    }

                    cancellationToken.WaitHandle.WaitOne(storage.Options.ExpirationCheckInterval);
                }
            }
        }
    }
}