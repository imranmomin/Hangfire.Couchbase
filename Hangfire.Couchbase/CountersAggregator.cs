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

using Hangfire.Couchbase.Documents;

namespace Hangfire.Couchbase
{
#pragma warning disable 618
    internal class CountersAggregator : IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog logger = LogProvider.For<CountersAggregator>();
        private const string DISTRIBUTED_LOCK_KEY = "countersaggragator";
        private static readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(5);
        private readonly TimeSpan checkInterval;
        private readonly CouchbaseStorage storage;

        public CountersAggregator(CouchbaseStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            checkInterval = storage.Options.CountersAggregateInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            logger.Debug("Aggregating records in 'Counter' table.");

            using (new CouchbaseDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
            {
                using (IBucket bucket = storage.Client.OpenBucket(storage.Options.Bucket))
                {
                    // context
                    BucketContext context = new BucketContext(bucket);

                    List<Counter> rawCounters = context.Query<Counter>()
                        .Where(c => c.DocumentType == DocumentTypes.Counter && c.Type == CounterTypes.Raw)
                        .ToList();

                    Dictionary<string, (int Value, DateTime? ExpireOn)> counters = rawCounters.GroupBy(c => c.Key)
                        .ToDictionary(k => k.Key, v => (Value: v.Sum(c => c.Value), ExpireOn: v.Max(c => c.ExpireOn)));

                    Array.ForEach(counters.Keys.ToArray(), key =>
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if (counters.TryGetValue(key, out var data))
                        {
                            Counter aggregated = context.Query<Counter>()
                                .FirstOrDefault(c =>
                                    c.DocumentType == DocumentTypes.Counter && c.Type == CounterTypes.Aggregrate &&
                                    c.Key == key);

                            if (aggregated == null)
                            {
                                aggregated = new Counter
                                {
                                    Key = key,
                                    Type = CounterTypes.Aggregrate,
                                    Value = data.Value,
                                    ExpireOn = data.ExpireOn
                                };
                            }
                            else
                            {
                                aggregated.Value += data.Value;
                                aggregated.ExpireOn = data.ExpireOn;
                            }

                            Task<IOperationResult<Counter>> task = bucket.UpsertAsync(aggregated.Id, aggregated);
                            Task continueTask = task.ContinueWith(t =>
                            {
                                if (t.Result.Success)
                                {
                                    List<IDocument<Counter>> s = rawCounters
                                        .Where(counter => counter.Key == key)
                                        .Select(counter => new Document<Counter> { Id = counter.Id, Content = counter })
                                        .Cast<IDocument<Counter>>()
                                        .ToList();

                                    using (IBucket removeBucket = storage.Client.OpenBucket(storage.Options.Bucket))
                                    {
                                        removeBucket.RemoveAsync(s).Wait(cancellationToken);
                                    }
                                }
                            }, cancellationToken, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Current);

                            continueTask.Wait(cancellationToken);
                        }
                    });
                }

                logger.Trace("Records from the 'Counter' table aggregated.");
                cancellationToken.WaitHandle.WaitOne(checkInterval);
            }
        }

        public override string ToString() => GetType().ToString();

    }
}
