using System;
using System.Net;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Couchbase;
using Hangfire.Couchbase.Documents;
using Hangfire.Server;
using Hangfire.Logging;

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
                List<Counter> rawCounters = storage.Client.CreateDocumentQuery<Counter>(storage.CollectionUri, queryOptions)
                    .Where(c => c.Type == CounterTypes.Raw && c.DocumentType == DocumentTypes.Counter)
                    .AsEnumerable()
                    .ToList();

                Dictionary<string, Tuple<int, DateTime?>> counters = rawCounters.GroupBy(c => c.Key)
                    .ToDictionary(k => k.Key, v => new Tuple<int, DateTime?>(v.Sum(c => c.Value), v.Max(c => c.ExpireOn)));

                Array.ForEach(counters.Keys.ToArray(), key =>
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    Tuple<int, DateTime?> data;
                    if (counters.TryGetValue(key, out data))
                    {
                        Counter aggregated = storage.Client.CreateDocumentQuery<Counter>(storage.CollectionUri, queryOptions)
                             .Where(c => c.Key == key && c.Type == CounterTypes.Aggregrate && c.DocumentType == DocumentTypes.Counter)
                             .AsEnumerable()
                             .FirstOrDefault();

                        if (aggregated == null)
                        {
                            aggregated = new Counter
                            {
                                Key = key,
                                Type = CounterTypes.Aggregrate,
                                Value = data.Item1,
                                ExpireOn = data.Item2
                            };
                        }
                        else
                        {
                            aggregated.Value += data.Item1;
                            aggregated.ExpireOn = data.Item2;
                        }

                        Task<ResourceResponse<Document<>>> task = storage.Client.UpsertDocumentWithRetriesAsync(storage.CollectionUri, aggregated);

                        Task continueTask = task.ContinueWith(t =>
                        {
                            if (t.Result.StatusCode == HttpStatusCode.Created || t.Result.StatusCode == HttpStatusCode.OK)
                            {
                                List<string> deleteCountersr = rawCounters.Where(c => c.Key == key).Select(c => c.Id).ToList();
                                Task<StoredProcedureResponse<bool>> procedureTask = storage.Client.ExecuteStoredProcedureAsync<bool>(spDeleteDocumentIfExistsUri, deleteCountersr);
                                procedureTask.Wait(cancellationToken);
                            }
                        }, cancellationToken, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Current);

                        continueTask.Wait(cancellationToken);
                    }
                });
            }

            logger.Trace("Records from the 'Counter' table aggregated.");
            cancellationToken.WaitHandle.WaitOne(checkInterval);
        }

        public override string ToString() => GetType().ToString();

    }
}
