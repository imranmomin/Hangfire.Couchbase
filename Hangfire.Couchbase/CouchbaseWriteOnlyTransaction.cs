using System;
using System.Linq;
using System.Collections.Generic;

using Couchbase;
using Couchbase.Core;
using Couchbase.Linq;
using Hangfire.States;
using Hangfire.Storage;

using Hangfire.Couchbase.Queue;
using Hangfire.Couchbase.Helper;
using Hangfire.Couchbase.Documents;

namespace Hangfire.Couchbase
{
    internal class CouchbaseWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly CouchbaseConnection connection;
        private readonly IBucket bucket;
        private readonly List<Action> commands = new List<Action>();

        public CouchbaseWriteOnlyTransaction(CouchbaseConnection connection)
        {
            this.connection = connection;
            CouchbaseStorage storage = connection.Storage;
            bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket);
        }

        private void QueueCommand(Action command) => commands.Add(command);
        public override void Commit() => commands.ForEach(command => command());
        public override void Dispose() => bucket.Dispose();

        #region Queue

        public override void AddToQueue(string queue, string jobId)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            IPersistentJobQueueProvider provider = connection.QueueProviders.GetProvider(queue);
            IPersistentJobQueue persistentQueue = provider.GetJobQueue();
            QueueCommand(() => persistentQueue.Enqueue(queue, jobId));
        }

        #endregion

        #region Counter

        public override void DecrementCounter(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                string id = $"{key}:COUNTER".ToUpperInvariant();
                bucket.Decrement(id, 1, 0);
            });
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (expireIn.Duration() != expireIn) throw new ArgumentException(@"The `expireIn` value must be positive.", nameof(expireIn));

            QueueCommand(() =>
            {
                string id = $"{key}:COUNTER".ToUpperInvariant();
                bucket.Decrement(id, 1, 0, expireIn);
            });
        }

        public override void IncrementCounter(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                string id = $"{key}:COUNTER".ToUpperInvariant();
                bucket.Increment(id, 1, 1);
            });
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (expireIn.Duration() != expireIn) throw new ArgumentException(@"The `expireIn` value must be positive.", nameof(expireIn));

            QueueCommand(() =>
            {
                string id = $"{key}:COUNTER".ToUpperInvariant();
                bucket.Increment(id, 1, 1, expireIn);
            });
        }

        #endregion

        #region Job

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
            if (expireIn.Duration() != expireIn) throw new ArgumentException(@"The `expireIn` value must be positive.", nameof(expireIn));

            QueueCommand(() =>
            {
                int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();
                bucket.MutateIn<Job>($"job:{jobId}")
                    .Upsert(j => j.ExpireOn, epoch, true)
                    .Execute();
            });
        }

        public override void PersistJob(string jobId)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            QueueCommand(() =>
            {
                bucket.MutateIn<Job>($"job:{jobId}")
                     .Remove(j => j.ExpireOn)
                     .Execute();
            });
        }

        #endregion

        #region State

        public override void SetJobState(string jobId, IState state)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            QueueCommand(() =>
            {
                // ReSharper disable once InconsistentNaming
                ulong job_id = Convert.ToUInt64(jobId);

                State data = new State
                {
                    JobId = job_id,
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedOn = DateTime.UtcNow,
                    Data = state.SerializeData()
                };

                IOperationResult stateResult = bucket.Insert(data.Id, data);
                if (stateResult.Success)
                {
                    bucket.MutateIn<Job>($"job:{jobId}")
                        .Upsert(j => j.StateId, data.Id, true)
                        .Upsert(j => j.StateName, data.Name, true)
                        .Execute();
                }
            });
        }

        public override void AddJobState(string jobId, IState state)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            QueueCommand(() =>
            {
                // ReSharper disable once InconsistentNaming
                ulong job_id = Convert.ToUInt64(jobId);

                State data = new State
                {
                    JobId = job_id,
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedOn = DateTime.UtcNow,
                    Data = state.SerializeData()
                };

                bucket.Insert(data.Id, data);
            });
        }

        #endregion

        #region Set

        public override void RemoveFromSet(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                BucketContext context = new BucketContext(bucket);
                IList<string> ids = context.Query<Set>()
                    .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                    .AsEnumerable()
                    .Where(s => s.Value == value)
                    .Select(s => s.Id)
                    .ToList();

                bucket.Remove(ids, TimeSpan.FromSeconds(30));
            });
        }

        public override void AddToSet(string key, string value) => AddToSet(key, value, 0.0);

        public override void AddToSet(string key, string value, double score)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                BucketContext context = new BucketContext(bucket);
                IList<string> ids = context.Query<Set>()
                    .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                    .AsEnumerable()
                    .Where(s => s.Value == value)
                    .Select(s => s.Id)
                    .ToList();

                if (ids.Count > 0)
                {
                    foreach (string id in ids)
                    {
                        bucket.MutateIn<Set>(id)
                            .Upsert(s => s.Score, score, true)
                            .Execute();
                    }
                }
                else
                {
                    Set data = new Set
                    {
                        Key = key,
                        Value = value,
                        Score = score,
                        CreatedOn = DateTime.UtcNow
                    };

                    bucket.Insert(data.Id, data);
                }
            });
        }

        public override void PersistSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                BucketContext context = new BucketContext(bucket);
                IList<string> ids = context.Query<Set>()
                    .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                    .Select(s => s.Id)
                    .ToList();

                foreach (string id in ids)
                {
                    bucket.MutateIn<Set>(id)
                        .Remove(s => s.ExpireOn)
                        .Execute();
                }
            });
        }

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();

                BucketContext context = new BucketContext(bucket);
                IList<string> ids = context.Query<Set>()
                    .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                    .Select(s => s.Id)
                    .ToList();

                foreach (string id in ids)
                {
                    bucket.MutateIn<Set>(id)
                        .Upsert(s => s.ExpireOn, epoch, true)
                        .Execute();
                }
            });
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));

            QueueCommand(() =>
            {
                foreach (string value in items)
                {
                    Set data = new Set
                    {
                        Key = key,
                        Value = value,
                        Score = 0.00,
                        CreatedOn = DateTime.UtcNow
                    };

                    bucket.Insert(data.Id, data);
                }
            });
        }

        public override void RemoveSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                BucketContext context = new BucketContext(bucket);
                IList<string> ids = context.Query<Set>()
                    .Where(h => h.DocumentType == DocumentTypes.Set && h.Key == key)
                    .Select(s => s.Id)
                    .ToList();

                bucket.Remove(ids, TimeSpan.FromSeconds(30));
            });
        }

        #endregion

        #region  Hash

        public override void RemoveHash(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                BucketContext context = new BucketContext(bucket);
                IList<string> ids = context.Query<Hash>()
                    .Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key)
                    .Select(h => h.Id)
                    .ToList();

                bucket.Remove(ids, TimeSpan.FromSeconds(30));
            });
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            QueueCommand(() =>
            {
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
            });
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();

                BucketContext context = new BucketContext(bucket);
                IList<string> ids = context.Query<Hash>()
                    .Where(s => s.DocumentType == DocumentTypes.Hash && s.Key == key)
                    .Select(h => h.Id)
                    .ToList();

                foreach (string id in ids)
                {
                    bucket.MutateIn<Hash>(id)
                        .Upsert(s => s.ExpireOn, epoch, true)
                        .Execute();
                }
            });
        }

        public override void PersistHash(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                BucketContext context = new BucketContext(bucket);
                IList<string> ids = context.Query<Hash>()
                    .Where(s => s.DocumentType == DocumentTypes.Hash && s.Key == key)
                    .Select(h => h.Id)
                    .ToList();

                foreach (string id in ids)
                {
                    bucket.MutateIn<Hash>(id)
                        .Remove(s => s.ExpireOn)
                        .Execute();
                }
            });
        }

        #endregion

        #region List

        public override void InsertToList(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                List data = new List
                {
                    Key = key,
                    Value = value,
                    CreatedOn = DateTime.UtcNow
                };

                bucket.Insert(data.Id, data);
            });
        }

        public override void RemoveFromList(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                BucketContext context = new BucketContext(bucket);
                IList<string> ids = context.Query<List>()
                    .Where(s => s.DocumentType == DocumentTypes.List && s.Key == key)
                    .AsEnumerable()
                    .Where(s => s.Value == value)
                    .Select(l => l.Id)
                    .ToList();

                bucket.Remove(ids, TimeSpan.FromSeconds(30));
            });
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                keepEndingAt += 1 - keepStartingFrom;

                BucketContext context = new BucketContext(bucket);
                string[] ids = context.Query<List>()
                    .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                    .OrderByDescending(l => l.CreatedOn)
                    .Skip(keepStartingFrom)
                    .Take(keepEndingAt)
                    .Select(l => l.Id)
                    .ToArray();

                bucket.Remove(ids, TimeSpan.FromSeconds(30));
            });
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();

                BucketContext context = new BucketContext(bucket);
                IList<string> ids = context.Query<List>()
                    .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                    .Select(l => l.Id)
                    .ToList();

                foreach (string id in ids)
                {
                    bucket.MutateIn<List>(id)
                        .Upsert(s => s.ExpireOn, epoch, true)
                        .Execute();
                }
            });
        }

        public override void PersistList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                BucketContext context = new BucketContext(bucket);
                IList<string> ids = context.Query<List>()
                    .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                    .Select(l => l.Id)
                    .ToList();

                foreach (string id in ids)
                {
                    bucket.MutateIn<List>(id)
                        .Remove(s => s.ExpireOn)
                        .Execute();
                }
            });
        }

        #endregion

    }
}