using System;
using System.Linq;
using System.Collections.Generic;

using Couchbase;
using Couchbase.Core;
using Couchbase.Linq;
using Hangfire.States;
using Hangfire.Storage;

using Hangfire.Couchbase.Queue;
using Hangfire.Couchbase.Documents;

namespace Hangfire.Couchbase
{
    internal class CouchbaseWriteOnlyTransaction : IWriteOnlyTransaction
    {
        private readonly CouchbaseConnection connection;
        private readonly IBucket bucket;
        private readonly List<Action> commands = new List<Action>();

        public CouchbaseWriteOnlyTransaction(CouchbaseConnection connection)
        {
            this.connection = connection;
            CouchbaseStorage storage = connection.Storage;
            bucket = storage.Client.OpenBucket(storage.Options.Bucket);
        }

        private void QueueCommand(Action command) => commands.Add(command);
        public void Commit() => commands.ForEach(command => command());
        public void Dispose() => bucket.Dispose();

        #region Queue

        public void AddToQueue(string queue, string jobId)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            IPersistentJobQueueProvider provider = connection.QueueProviders.GetProvider(queue);
            IPersistentJobQueue persistentQueue = provider.GetJobQueue();
            QueueCommand(() => persistentQueue.Enqueue(queue, jobId));
        }

        #endregion

        #region Counter

        public void DecrementCounter(string key)
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

        public void DecrementCounter(string key, TimeSpan expireIn)
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
                    ExpireOn = DateTime.UtcNow.Add(expireIn)
                };

                bucket.Insert(data.Id, data);
            });
        }

        public void IncrementCounter(string key)
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

        public void IncrementCounter(string key, TimeSpan expireIn)
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
                    ExpireOn = DateTime.UtcNow.Add(expireIn)
                };

                bucket.Insert(data.Id, data);
            });
        }

        #endregion

        #region Job

        public void ExpireJob(string jobId, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
            if (expireIn.Duration() != expireIn) throw new ArgumentException(@"The `expireIn` value must be positive.", nameof(expireIn));

            QueueCommand(() =>
            {
                IDocumentResult<Job> result = bucket.GetDocument<Job>(jobId);
                if (result.Success && result.Content != null)
                {
                    Job data = result.Content;
                    data.ExpireOn = DateTime.UtcNow.Add(expireIn);
                    bucket.Upsert(jobId, data);
                }
            });
        }

        public void PersistJob(string jobId)
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

        public void SetJobState(string jobId, IState state)
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

        public void AddJobState(string jobId, IState state)
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

        public void RemoveFromSet(string key, string value)
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

        public void AddToSet(string key, string value) => AddToSet(key, value, 0.0);

        public void AddToSet(string key, string value, double score)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                BucketContext context = new BucketContext(bucket);
                Set data = context.Query<Set>().FirstOrDefault(s => s.DocumentType == DocumentTypes.Set && s.Key == key && s.Value == value);

                if (data != null)
                {
                    data.Key = key;
                    data.Value = value;
                    data.Score = score;
                }
                else
                {
                    data = new Set
                    {
                        Key = key,
                        Value = value,
                        Score = score
                    };
                }

                bucket.Upsert(data.Id, data);
            });
        }

        #endregion

        #region  Hash

        public void RemoveHash(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                BucketContext context = new BucketContext(bucket);
                Hash[] hashes = context.Query<Hash>()
                    .Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key)
                    .ToArray();

                Array.ForEach(hashes, hash => bucket.Remove(hash.Id));
            });
        }

        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            QueueCommand(() =>
            {
                Hash[] sources = keyValuePairs.Select(k => new Hash
                {
                    Key = key,
                    Field = k.Key,
                    Value = k.Value
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

        #endregion

        #region List

        public void InsertToList(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                List data = new List
                {
                    Key = key,
                    Value = value
                };

                bucket.Insert(data.Id, data);
            });
        }

        public void RemoveFromList(string key, string value)
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

        public void TrimList(string key, int keepStartingFrom, int keepEndingAt)
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

        #endregion

    }
}