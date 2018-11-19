using System;
using System.Threading;

using Couchbase;
using Couchbase.Core;
using Hangfire.Logging;
using Hangfire.Storage;

using Hangfire.Couchbase.Helper;

// ReSharper disable once CheckNamespace
namespace Hangfire.Couchbase.Queue
{
    internal class FetchedJob : IFetchedJob
    {
        private readonly ILog logger = LogProvider.GetLogger(typeof(FetchedJob));
        private readonly IBucket bucket;
        private readonly object syncRoot = new object();
        private readonly Timer timer;
        private bool disposed;
        private bool removedFromQueue;
        private bool reQueued;

        public FetchedJob(CouchbaseStorage storage, Documents.Queue data)
        {
            bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket);

            Id = data.Id;
            JobId = data.JobId;

            TimeSpan keepAliveInterval = TimeSpan.FromMinutes(5);
            timer = new Timer(KeepAliveJobCallback, null, keepAliveInterval, keepAliveInterval);
        }

        private string Id { get; }

        public string JobId { get; }

        public void Dispose()
        {
            if (disposed) return;
            disposed = true;

            timer?.Dispose();

            lock (syncRoot)
            {
                if (!removedFromQueue && !reQueued)
                {
                    Requeue();
                }
            }

            bucket?.Dispose();
        }

        public void RemoveFromQueue()
        {
            lock (syncRoot)
            {
                IOperationResult result = bucket.Remove(Id);
                removedFromQueue = result.Success;
            }
        }

        public void Requeue()
        {
            lock (syncRoot)
            {
                IDocumentFragment<Documents.Queue> result = bucket.MutateIn<Documents.Queue>(Id)
                    .Remove(q => q.FetchedAt)
                    .Execute();

                reQueued = result.Success;
            }
        }

        private void KeepAliveJobCallback(object obj)
        {
            lock (syncRoot)
            {
                if (reQueued || removedFromQueue) return;

                try
                {
                    bucket.MutateIn<Documents.Queue>(Id)
                        .Upsert(q => q.FetchedAt, DateTime.UtcNow.ToEpoch(), false)
                        .Execute();

                    logger.Trace($"Keep-alive query for job: {Id} sent");
                }
                catch (Exception ex)
                {
                    logger.DebugException($"Unable to execute keep-alive query for job: {Id}", ex);
                }
            }
        }
    }
}