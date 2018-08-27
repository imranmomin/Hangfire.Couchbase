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
                Counter data = new Counter
                {
                    Key = key,
                    Type = CounterTypes.Raw,
                    Value = -1
                };

                bucket.Insert(data.Id, data);
            });
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (expireIn.Duration() != expireIn) throw new ArgumentException(@"The `expireIn` value must be positive.", nameof(expireIn));

            QueueCommand(() =>
            {
                Counter data = new Counter
                {
                    Key = key,
                    Type = CounterTypes.Raw,
                    Value = -1,
                    ExpireOn = DateTime.UtcNow.Add(expireIn).ToEpoch()
                };

                bucket.Insert(data.Id, data);
            });
        }

        public override void IncrementCounter(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                Counter data = new Counter
                {
                    Key = key,
                    Type = CounterTypes.Raw,
                    Value = 1
                };

                bucket.Insert(data.Id, data);
            });
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (expireIn.Duration() != expireIn) throw new ArgumentException(@"The `expireIn` value must be positive.", nameof(expireIn));

            QueueCommand(() =>
            {
                Counter data = new Counter
                {
                    Key = key,
                    Type = CounterTypes.Raw,
                    Value = 1,
                    ExpireOn = DateTime.UtcNow.Add(expireIn).ToEpoch()
                };

                bucket.Insert(data.Id, data);
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
                IDocumentResult<Job> result = bucket.GetDocument<Job>(jobId);
                if (result.Success && result.Content != null)
                {
                    Job data = result.Content;
                    data.ExpireOn = DateTime.UtcNow.Add(expireIn).ToEpoch();
                    bucket.Upsert(jobId, data);
                }
            });
        }

        public override void PersistJob(string jobId)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            QueueCommand(() =>
            {
                IDocumentResult<Job> result = bucket.GetDocument<Job>(jobId);
                if (result.Success && result.Content != null)
                {
                    Job data = result.Content;
                    data.ExpireOn = null;
                    bucket.Upsert(jobId, data);
                }
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
                State data = new State
                {
                    JobId = jobId,
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedOn = DateTime.UtcNow,
                    Data = state.SerializeData()
                };

                IOperationResult stateResult = bucket.Insert(data.Id, data);
                if (stateResult.Success)
                {
                    IDocumentResult<Job> result = bucket.GetDocument<Job>(jobId);
                    if (result.Success && result.Content != null)
                    {
                        Job job = result.Content;
                        job.StateId = data.Id;
                        job.StateName = data.Name;

                        bucket.Upsert(jobId, job);
                    }
                }
            });
        }

        public override void AddJobState(string jobId, IState state)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            QueueCommand(() =>
            {
                State data = new State
                {
                    JobId = jobId,
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
                string id = context.Query<Set>()
                    .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key && s.Value == value)
                    .Select(s => s.Id)
                    .FirstOrDefault();

                if (!string.IsNullOrEmpty(id))
                {
                    bucket.Remove(id);
                }
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
                Set data = context.Query<Set>().FirstOrDefault(s => s.DocumentType == DocumentTypes.Set && s.Key == key && s.Value == value);

                if (data != null)
                {
                    data.Score = score;
                }
                else
                {
                    data = new Set
                    {
                        Key = key,
                        Value = value,
                        Score = score,
                        CreatedOn = DateTime.UtcNow
                    };
                }

                bucket.Upsert(data.Id, data);
            });
        }

        public override void PersistSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                BucketContext context = new BucketContext(bucket);
                Set[] sets = context.Query<Set>()
                    .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                    .ToArray();

                foreach (Set set in sets)
                {
                    set.ExpireOn = null;
                    bucket.Upsert(set.Id, set);
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
                Set[] sets = context.Query<Set>()
                    .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                    .ToArray();

                foreach (Set set in sets)
                {
                    set.ExpireOn = epoch;
                    bucket.Upsert(set.Id, set);
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
                string[] ids = context.Query<Set>()
                    .Where(h => h.DocumentType == DocumentTypes.Set && h.Key == key)
                    .Select(s => s.Id)
                    .ToArray();

                Array.ForEach(ids, id => bucket.Remove(id));
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
                string[] ids = context.Query<Hash>()
                    .Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key)
                    .Select(h => h.Id)
                    .ToArray();

                Array.ForEach(ids, id => bucket.Remove(id));
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
                IEnumerable<Hash> hashes = context.Query<Hash>().Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key);

                foreach (Hash source in sources)
                {
                    Hash hash = hashes.FirstOrDefault(h => h.Field == source.Field);
                    if (hash != null && hash.Field == source.Field) source.Id = hash.Id;
                    bucket.Upsert(source.Id, source);
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
                Hash[] hashes = context.Query<Hash>()
                    .Where(s => s.DocumentType == DocumentTypes.Hash && s.Key == key)
                    .ToArray();

                foreach (Hash hash in hashes)
                {
                    hash.ExpireOn = epoch;
                    bucket.Upsert(hash.Id, hash);
                }
            });
        }

        public override void PersistHash(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                BucketContext context = new BucketContext(bucket);
                Hash[] hashes = context.Query<Hash>()
                    .Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key)
                    .ToArray();

                foreach (Hash hash in hashes)
                {
                    hash.ExpireOn = null;
                    bucket.Upsert(hash.Id, hash);
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
                string id = context.Query<List>()
                    .Where(s => s.DocumentType == DocumentTypes.List && s.Key == key && s.Value == value)
                    .Select(s => s.Id)
                    .FirstOrDefault();

                if (!string.IsNullOrEmpty(id))
                {
                    bucket.Remove(id);
                }
            });
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                BucketContext context = new BucketContext(bucket);
                List[] lists = context.Query<List>()
                    .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                    .ToArray();

                for (int index = 0; index < lists.Length; index++)
                {
                    if (index < keepStartingFrom || index > keepEndingAt)
                    {
                        List list = lists.ElementAt(index);
                        bucket.Remove(list.Id);
                    }
                }
            });
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();
                
                BucketContext context = new BucketContext(bucket);
                List[] lists = context.Query<List>()
                    .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                    .ToArray();

                foreach (List list in lists)
                {
                    list.ExpireOn = epoch;
                    bucket.Upsert(list.Id, list);
                }
            });
        }

        public override void PersistList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                BucketContext context = new BucketContext(bucket);
                List[] lists = context.Query<List>()
                    .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                    .ToArray();

                foreach (List list in lists)
                {
                    list.ExpireOn = null;
                    bucket.Upsert(list.Id, list);
                }
            });
        }

        #endregion

    }
}