using System;
using System.Linq;
using System.Collections.Generic;

using Couchbase.Core;
using Couchbase.Linq;
using Hangfire.Couchbase.Documents;

namespace Hangfire.Couchbase.Queue
{
    internal class JobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly CouchbaseStorage storage;
        private readonly List<string> queuesCache = new List<string>();
        private DateTime cacheUpdated;
        private readonly object cacheLock = new object();
        private static readonly TimeSpan queuesCacheTimeout = TimeSpan.FromSeconds(5);

        public JobQueueMonitoringApi(CouchbaseStorage storage) => this.storage = storage;

        public IEnumerable<string> GetQueues()
        {
            lock (cacheLock)
            {
                if (queuesCache.Count == 0 || cacheUpdated.Add(queuesCacheTimeout) < DateTime.UtcNow)
                {
                    using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
                    {
                        BucketContext context = new BucketContext(bucket);
                        IEnumerable<string> result = context.Query<Documents.Queue>()
                            .Where(q => q.DocumentType == DocumentTypes.Queue)
                            .Select(q => q.Name)
                            .AsEnumerable()
                            .Distinct();

                        queuesCache.Clear();
                        queuesCache.AddRange(result);
                        cacheUpdated = DateTime.UtcNow;
                    }
                }

                return queuesCache.ToList();
            }
        }

        public int GetEnqueuedCount(string queue)
        {
            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                BucketContext context = new BucketContext(bucket);
                return context.Query<Documents.Queue>().Count(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue);
            }
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                BucketContext context = new BucketContext(bucket);
                return context.Query<Documents.Queue>()
                    .Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue && q.FetchedAt.HasValue == false)
                    .OrderBy(q => q.CreatedOn)
                    .Skip(from + 1).Take(perPage + 1)
                    .Select(c => c.JobId)
                    .AsEnumerable();
            }
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage)
        {
            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                BucketContext context = new BucketContext(bucket);
                return context.Query<Documents.Queue>()
                    .Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue && q.FetchedAt.HasValue)
                    .OrderBy(q => q.CreatedOn)
                    .Skip(from + 1).Take(perPage + 1)
                    .Select(c => c.JobId)
                    .AsEnumerable();
            }
        }

        public (int? EnqueuedCount, int? FetchedCount) GetEnqueuedAndFetchedCount(string queue)
        {
            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                BucketContext context = new BucketContext(bucket);
                (int Fetched, int Enqueued) result = context.Query<Documents.Queue>()
                    .Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue)
                    .AsEnumerable()
                    .GroupBy(q => q.Name)
                    .Select(v => (Fetched: v.Sum(q => q.FetchedAt.HasValue ? 1 : 0), Enqueued: v.Sum(q => q.FetchedAt.HasValue ? 0 : 1)))
                    .FirstOrDefault();

                return result;
            }
        }
    }
}