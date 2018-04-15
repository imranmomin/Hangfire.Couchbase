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
            State state = context.Query<State>().FirstOrDefault(s => s.JobId == jobId && s.DocumentType == DocumentTypes.State);

            if (state != null)
            {
                return new StateData
                {
                    Name = state.Name,
                    Reason = state.Reason,
                    Data = state.Data
                };
            }

            return null;
        }

        #endregion

        #region Parameter

        public override string GetJobParameter(string id, string name)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            IDocumentResult<Documents.Job> result = bucket.GetDocument<Documents.Job>(id);
            return result.Success && result.Content != null && result.Content.Parameters.TryGetValue(name, out string value) ? value : null;
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));


            IDocumentResult<Documents.Job> result = bucket.GetDocument<Documents.Job>(id);
            if (result.Success && result.Content != null)
            {
                Documents.Job data = result.Content;
                if (data.Parameters.ContainsKey(name))
                {
                    data.Parameters[name] = value;
                }
                else
                {
                    data.Parameters.Add(name, value);
                }
                bucket.Upsert(id, data);
            }
        }

        #endregion

        #region Set

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            int? expireOn = context.Query<Set>()
                .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                .OrderByDescending(s => s.ExpireOn)
                .Select(s => s.ExpireOn)
                .FirstOrDefault();

            return expireOn.HasValue ? expireOn.Value.ToDateTime() - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            return context.Query<Set>()
                .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                .Select(c => c.Value)
                .AsEnumerable()
                .Skip(startingFrom).Take(endingAt)
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
                 .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                 .OrderBy(s => s.Score)
                 .AsEnumerable()
                 .Skip((int)fromScore)
                 .Take((int)toScore)
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
            Documents.Server server = bucketContext.Query<Documents.Server>().FirstOrDefault(s => s.DocumentType == DocumentTypes.Server && s.ServerId == serverId);

            if (server == null)
            {
                server = new Documents.Server
                {
                    ServerId = serverId,
                    Workers = context.WorkerCount,
                    Queues = context.Queues,
                    CreatedOn = DateTime.UtcNow,
                    LastHeartbeat = DateTime.UtcNow
                };
            }
            else
            {
                server.Workers = context.WorkerCount;
                server.Queues = context.Queues;
                server.LastHeartbeat = DateTime.UtcNow;
            }

            bucket.Upsert(server.Id, server);
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            BucketContext context = new BucketContext(bucket);
            Documents.Server server = context.Query<Documents.Server>().FirstOrDefault(s => s.DocumentType == DocumentTypes.Server && s.ServerId == serverId);

            if (server != null)
            {
                server.LastHeartbeat = DateTime.UtcNow;
                bucket.Upsert(server.Id, server);
            }
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            BucketContext context = new BucketContext(bucket);
            Documents.Server server = context.Query<Documents.Server>().FirstOrDefault(s => s.DocumentType == DocumentTypes.Server && s.ServerId == serverId);
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

            DateTime lastHeartbeat = DateTime.UtcNow.Add(timeOut.Negate());
            BucketContext context = new BucketContext(bucket);
            string[] documentIds = context.Query<Documents.Server>()
                .Where(s => s.DocumentType == DocumentTypes.Server && s.LastHeartbeat < lastHeartbeat)
                .Select(s => s.Id)
                .ToArray();

            Array.ForEach(documentIds, id => bucket.Remove(id));
            return documentIds.Length;
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
            IEnumerable<Hash> hashes = context.Query<Hash>().Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key);

            foreach (Hash source in sources)
            {
                Hash hash = hashes.FirstOrDefault(h => h.Field == source.Field);
                if (hash != null && hash.Field == source.Field) source.Id = hash.Id;
                bucket.Upsert(source.Id, source);
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
                .OrderByDescending(h => h.ExpireOn)
                .Select(h => h.ExpireOn)
                .FirstOrDefault();

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
                .Select(l => l.Value)
                .ToList();
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            return context.Query<List>()
                .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                .OrderByDescending(l => l.ExpireOn)
                .Select(l => l.Value)
                .AsEnumerable()
                .Skip(startingFrom).Take(endingAt)
                .ToList();
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            BucketContext context = new BucketContext(bucket);
            int? expireOn = context.Query<List>()
                .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                .OrderByDescending(l => l.ExpireOn)
                .Select(l => l.ExpireOn)
                .FirstOrDefault();

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