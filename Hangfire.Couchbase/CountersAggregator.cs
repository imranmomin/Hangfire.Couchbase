using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using Couchbase;
using Couchbase.Core;
using Couchbase.Linq;
using Hangfire.Server;
using Hangfire.Logging;

using Hangfire.Couchbase.Helper;
using Hangfire.Couchbase.Documents;

namespace Hangfire.Couchbase
{
#pragma warning disable 618
    internal class CountersAggregator : IServerComponent
#pragma warning restore 618
    {
        private readonly ILog logger = LogProvider.For<CountersAggregator>();
        private const string DISTRIBUTED_LOCK_KEY = "locks:counters:aggragator";
        private readonly TimeSpan defaultLockTimeout;
        private readonly CouchbaseStorage storage;

        public CountersAggregator(CouchbaseStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            defaultLockTimeout = TimeSpan.FromSeconds(30) + storage.Options.CountersAggregateInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            using (new CouchbaseDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
            {
                logger.Trace("Aggregating records in 'Counter' table.");

                using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
                {
                    // context
                    BucketContext context = new BucketContext(bucket);

                    List<Counter> rawCounters = context.Query<Counter>()
                        .Where(c => c.DocumentType == DocumentTypes.Counter && c.Type == CounterTypes.Raw)
                        .ToList();

                    Dictionary<string, (int Value, int? ExpireOn, List<Counter> Counters)> counters = rawCounters.GroupBy(c => c.Key)
                        .ToDictionary(k => k.Key, v => (Value: v.Sum(c => c.Value), ExpireOn: v.Max(c => c.ExpireOn), Counters: v.ToList()));

                    foreach (string key in counters.Keys)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if (counters.TryGetValue(key, out var data))
                        {
                            Counter aggregated;
                            string id = $"{key}:{CounterTypes.Aggregate}".GenerateHash();
                            IOperationResult<Counter> operation = bucket.Get<Counter>(id, TimeSpan.FromMinutes(1));

                            if (operation.Success == false && operation.Value == null)
                            {
                                aggregated = new Counter
                                {
                                    Id = id,
                                    Key = key,
                                    Type = CounterTypes.Aggregate,
                                    Value = data.Value,
                                    ExpireOn = data.ExpireOn
                                };
                            }
                            else if (operation.Success && operation.Value.Type == CounterTypes.Aggregate)
                            {
                                aggregated = operation.Value;
                                aggregated.Value += data.Value;
                                aggregated.ExpireOn = new[] { aggregated.ExpireOn, data.ExpireOn }.Max();
                            }
                            else
                            {
                                logger.Warn($"Document with ID: {id} is a {operation.Value.Type.ToString()} type");
                                continue;
                            }

                            IOperationResult<Counter> result = bucket.Upsert(aggregated.Id, aggregated);
                            if (result.Success)
                            {
                                IList<string> ids = data.Counters
                                    .Select(counter => counter.Id)
                                    .ToList();

                                bucket.Remove(ids, new ParallelOptions { CancellationToken = cancellationToken }, TimeSpan.FromMinutes(1));
                                logger.Trace($"Total {ids.Count} records from the 'Counter:{aggregated.Key}' were aggregated.");
                            }
                        }
                    }
                }

                logger.Trace("Records from the 'Counter' table aggregated.");
            }

            cancellationToken.WaitHandle.WaitOne(storage.Options.CountersAggregateInterval);
        }

        public override string ToString() => GetType().ToString();

    }
}
