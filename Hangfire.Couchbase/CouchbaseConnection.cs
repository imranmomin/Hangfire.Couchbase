using System;
using System.Linq;
using System.Threading;
using System.Collections.Generic;

using Couchbase;
using Couchbase.Core;
using Couchbase.Linq;
using Hangfire.Server;
using Hangfire.Common;
using Hangfire.Storage;

using Hangfire.Couchbase.Queue;
using Hangfire.Couchbase.Helper;
using Hangfire.Couchbase.Documents;

namespace Hangfire.Couchbase
{
    internal sealed class CouchbaseConnection : JobStorageConnection
    {
        private readonly IBucket bucket;

        public CouchbaseStorage Storage { get; }
        public PersistentJobQueueProviderCollection QueueProviders { get; }

        public CouchbaseConnection(CouchbaseStorage storage)
        {
            Storage = storage;
            QueueProviders = storage.QueueProviders;

            bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout) => new CouchbaseDistributedLock(resource, timeout, Storage);
        public override IWriteOnlyTransaction CreateWriteTransaction() => new CouchbaseWriteOnlyTransaction(this);

        public override void Dispose() => bucket?.Dispose();

        #region Job

        public override string CreateExpiredJob(Common.Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            InvocationData invocationData = InvocationData.Serialize(job);
            Documents.Job entityJob = new Documents.Job
            {
                InvocationData = invocationData,
                Arguments = invocationData.Arguments,
                CreatedOn = createdAt,
                ExpireOn = createdAt.Add(expireIn).ToEpoch(),
                Parameters = parameters
            };

            IOperationResult<Documents.Job> response = bucket.Insert(entityJob.Id, entityJob);
            if (response.Success) return entityJob.Id;

            return string.Empty;
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0) throw new ArgumentNullException(nameof(queues));

            IPersistentJobQueueProvider[] providers = queues.Select(q => QueueProviders.GetProvider(q))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException($"Multiple provider instances registered for queues: {string.Join(", ", queues)}. You should choose only one type of persistent queues per server instance.");
            }

            IPersistentJobQueue persistentQueue = providers.Single().GetJobQueue();
            IFetchedJob queue = persistentQueue.Dequeue(queues, cancellationToken);
            return queue;
        }

        public override JobData GetJobData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            IDocumentResult<Documents.Job> result = bucket.GetDocument<Documents.Job>(jobId);
            if (result.Success && result.Content != null)
            {
                Documents.Job data = result.Content;
                InvocationData invocationData = data.InvocationData;
                invocationData.Arguments = data.Arguments;

                Common.Job job = null;
                JobLoadException loadException = null;

                try
                {
                    job = invocationData.Deserialize();
                }
                catch (JobLoadException ex)
                {
                    loadException = ex;
                }

                return new JobData
                {
                    Job = job,
                    State = data.StateName,
                    CreatedAt = data.CreatedOn,
                    LoadException = loadException
                };
            }

            return null;
        }

        public override StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            BucketContext context = new BucketContext(bucket);
            IQueryable<State> states = context.Query<State>().Where(s => s.DocumentType == DocumentTypes.State && s.JobId == jobId);

            StateData stateData = context.Query<Documents.Job>()
                .Where(j => j.Id == jobId)
                .Join(states, job => job.StateId, s => N1QlFunctions.Key(s), (job, s) => new StateData
                {
                    Name = s.Name,
                    Reason = s.Reason,
                    Data = s.Data
                })
                .SingleOrDefault();

            return stateData;
        }

        #endregion

        #region Parameter

        public override string GetJobParameter(string id, string name)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            IDocumentFragment<Documents.Job> result = bucket.LookupIn<Documents.Job>(id)
                .Get($"parameters.{name}")
                .Execute();

            return result.Success ? result.Content($"parameters.{name}").ToString() : null;
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            bucket.MutateIn<Documents.Job>(id)
                .Upsert($"parameters.{name}", value)
                .Execute();
        }

        #endregion

        #region Set

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            int? expireOn = context.Query<Set>()
                .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                .Min(s => s.ExpireOn);

            return expireOn.HasValue ? expireOn.Value.ToDateTime() - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            return context.Query<Set>()
                .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                .OrderBy(s => s.Score)
                .ThenBy(s => s.CreatedOn)
                .AsEnumerable()
                .Select((s, i) => new { s.Value, Index = i })
                .Where(s => s.Index >= startingFrom && s.Index <= endingAt)
                .Select(s => s.Value)
                .ToList();
        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            return context.Query<Counter>()
                .Where(s => s.DocumentType == DocumentTypes.Counter && s.Key == key)
                .Sum(s => s.Value);
        }

        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            return context.Query<Set>()
                .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                .LongCount();
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            IEnumerable<string> sets = context.Query<Set>()
                .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                .Select(s => s.Value)
                .AsEnumerable();

            return new HashSet<string>(sets);
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            BucketContext context = new BucketContext(bucket);
            return context.Query<Set>()
                .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key && s.Score >= (int)fromScore && s.Score <= (int)toScore)
                .OrderBy(s => s.Score)
                .Select(s => s.Value)
                .FirstOrDefault();
        }

        #endregion

        #region Server

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            if (context == null) throw new ArgumentNullException(nameof(context));

            BucketContext bucketContext = new BucketContext(bucket);
            Documents.Server server = bucketContext.Query<Documents.Server>()
                .FirstOrDefault(s => s.DocumentType == DocumentTypes.Server && s.ServerId == serverId);

            if (server == null)
            {
                server = new Documents.Server
                {
                    ServerId = serverId,
                    Workers = context.WorkerCount,
                    Queues = context.Queues,
                    CreatedOn = DateTime.UtcNow,
                    LastHeartbeat = DateTime.UtcNow.ToEpoch()
                };
            }
            else
            {
                server.Workers = context.WorkerCount;
                server.Queues = context.Queues;
                server.LastHeartbeat = DateTime.UtcNow.ToEpoch();
            }

            bucket.Upsert(server.Id, server);
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            BucketContext context = new BucketContext(bucket);
            Documents.Server server = context.Query<Documents.Server>()
                .FirstOrDefault(s => s.DocumentType == DocumentTypes.Server && s.ServerId == serverId);

            if (server != null)
            {
                bucket.MutateIn<Documents.Server>(server.Id)
                    .Upsert(s => s.LastHeartbeat, DateTime.UtcNow.ToEpoch(), false)
                    .Execute();
            }
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            BucketContext context = new BucketContext(bucket);
            Documents.Server server = context.Query<Documents.Server>()
                .FirstOrDefault(s => s.DocumentType == DocumentTypes.Server && s.ServerId == serverId);

            if (server != null)
            {
                bucket.Remove(server.Id);
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException(@"invalid timeout", nameof(timeOut));
            }

            int lastHeartbeat = DateTime.UtcNow.Add(timeOut.Negate()).ToEpoch();
            BucketContext context = new BucketContext(bucket);
            IList<string> ids = context.Query<Documents.Server>()
                .Where(s => s.DocumentType == DocumentTypes.Server && s.LastHeartbeat < lastHeartbeat)
                .Select(s => s.Id)
                .ToArray();

            bucket.Remove(ids, TimeSpan.FromSeconds(30));
            return ids.Count;
        }

        #endregion

        #region Hash

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            return context.Query<Hash>()
                .Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key)
                .Select(h => new { h.Field, h.Value })
                .ToDictionary(h => h.Field, h => h.Value);
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            Hash[] sources = keyValuePairs.Select(k => new Hash
            {
                Key = key,
                Field = k.Key,
                Value = k.Value.TryParseToEpoch()
            }).ToArray();

            BucketContext context = new BucketContext(bucket);
            IQueryable<Hash> hashes = context.Query<Hash>()
                .Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key);

            foreach (Hash source in sources)
            {
                var hash = hashes.SingleOrDefault(h => h.Field == source.Field);
                if (hash != null)
                {
                    bucket.MutateIn<Hash>(hash.Id)
                        .Upsert(h => h.Value, source.Value, true)
                        .Execute();
                }
                else
                {
                    bucket.Insert(source.Id, source);
                }
            }
        }

        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            return context.Query<Hash>()
                .Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key)
                .LongCount();
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (name == null) throw new ArgumentNullException(nameof(name));

            BucketContext context = new BucketContext(bucket);
            return context.Query<Hash>()
                .Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key && h.Field == name)
                .Select(h => h.Value)
                .FirstOrDefault();
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            int? expireOn = context.Query<Hash>()
                .Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key)
                .Min(h => h.ExpireOn);

            return expireOn.HasValue ? expireOn.Value.ToDateTime() - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        #endregion

        #region List

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            return context.Query<List>()
                .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                .OrderByDescending(l => l.CreatedOn)
                .Select(l => l.Value)
                .ToList();
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            return context.Query<List>()
                .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                .OrderByDescending(l => l.CreatedOn)
                .AsEnumerable()
                .Select((l, i) => new { l.Value, Index = i })
                .Where(l => l.Index >= startingFrom && l.Index <= endingAt)
                .Select(l => l.Value)
                .ToList();
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            int? expireOn = context.Query<List>()
                .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                .Min(l => l.ExpireOn);

            return expireOn.HasValue ? expireOn.Value.ToDateTime() - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            return context.Query<List>()
                .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                .LongCount();
        }

        #endregion

    }
}