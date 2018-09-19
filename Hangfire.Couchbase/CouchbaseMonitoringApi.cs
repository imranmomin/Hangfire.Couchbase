using System;
using System.Linq;
using System.Collections.Generic;

using Couchbase;
using Couchbase.Core;
using Couchbase.Linq;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

using Hangfire.Couchbase.Queue;
using Hangfire.Couchbase.Helper;
using Hangfire.Couchbase.Documents;

namespace Hangfire.Couchbase
{
    internal sealed class CouchbaseMonitoringApi : IMonitoringApi
    {
        private readonly CouchbaseStorage storage;

        public CouchbaseMonitoringApi(CouchbaseStorage storage) => this.storage = storage;

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            List<QueueWithTopEnqueuedJobsDto> queueJobs = new List<QueueWithTopEnqueuedJobsDto>();

            var tuples = storage.QueueProviders
                .Select(x => x.GetJobQueueMonitoringApi())
                .SelectMany(x => x.GetQueues(), (monitoring, queue) => new { Monitoring = monitoring, Queue = queue })
                .OrderBy(x => x.Queue)
                .ToArray();

            foreach (var tuple in tuples)
            {
                (int? EnqueuedCount, int? FetchedCount) counters = tuple.Monitoring.GetEnqueuedAndFetchedCount(tuple.Queue);
                JobList<EnqueuedJobDto> jobs = EnqueuedJobs(tuple.Queue, 0, 5);

                queueJobs.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Length = counters.EnqueuedCount ?? 0,
                    Fetched = counters.FetchedCount ?? 0,
                    Name = tuple.Queue,
                    FirstJobs = jobs
                });
            }

            return queueJobs;
        }

        public IList<ServerDto> Servers()
        {
            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                BucketContext context = new BucketContext(bucket);
                return context.Query<Documents.Server>()
                    .Where(s => s.DocumentType == DocumentTypes.Server)
                    .OrderByDescending(s => s.CreatedOn)
                    .AsEnumerable()
                    .Select(server => new ServerDto
                    {
                        Name = server.ServerId,
                        Heartbeat = server.LastHeartbeat,
                        Queues = server.Queues,
                        StartedAt = server.CreatedOn,
                        WorkersCount = server.Workers
                    }).ToList();
            }
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                IDocumentResult<Documents.Job> result = bucket.GetDocument<Documents.Job>(jobId);
                if (result.Success && result.Content != null)
                {
                    Documents.Job job = result.Content;
                    InvocationData invocationData = job.InvocationData;
                    invocationData.Arguments = job.Arguments;

                    BucketContext context = new BucketContext(bucket);
                    List<StateHistoryDto> states = context.Query<State>()
                        .Where(s => s.DocumentType == DocumentTypes.State && s.JobId == jobId)
                        .OrderByDescending(s => s.CreatedOn)
                        .AsEnumerable()
                        .Select(s => new StateHistoryDto
                        {
                            Data = s.Data,
                            CreatedAt = s.CreatedOn,
                            Reason = s.Reason,
                            StateName = s.Name
                        }).ToList();

                    return new JobDetailsDto
                    {
                        Job = invocationData.Deserialize(),
                        CreatedAt = job.CreatedOn,
                        ExpireAt = job.ExpireOn?.ToDateTime(),
                        Properties = job.Parameters,
                        History = states
                    };
                }
            }

            return null;
        }

        public StatisticsDto GetStatistics()
        {
            Dictionary<string, long> results = new Dictionary<string, long>();

            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                //query context
                BucketContext context = new BucketContext(bucket);

                // get counts of jobs groupby on state
                Dictionary<string, long> states = context.Query<Documents.Job>()
                    .Where(j => j.DocumentType == DocumentTypes.Job)
                    .Select(j => j.StateName)
                    .AsEnumerable()
                    .Where(j => !string.IsNullOrEmpty(j))
                    .GroupBy(j => j)
                    .ToDictionary(g => g.Key, g => g.LongCount());

                results = results.Concat(states).ToDictionary(k => k.Key, v => v.Value);

                // get counts of servers
                long servers = context.Query<Documents.Server>()
                    .Where(s => s.DocumentType == DocumentTypes.Server)
                    .LongCount();

                results.Add("Servers", servers);

                // get sum of stats:succeeded counters  raw / aggregate
                Dictionary<string, long> counters = context.Query<Counter>()
                    .Where(c => c.DocumentType == DocumentTypes.Counter && (c.Key == "stats:succeeded" || c.Key == "stats:deleted"))
                    .AsEnumerable()
                    .GroupBy(c => c.Key)
                    .ToDictionary(g => g.Key, g => (long)g.Sum(c => c.Value));

                results = results.Concat(counters).ToDictionary(k => k.Key, v => v.Value);

                // get recurring-jobs counts
                long count = context.Query<Set>()
                    .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == "recurring-jobs")
                    .LongCount();

                results.Add("recurring-jobs", count);

                long GetValueOrDefault(string key) => results.Where(r => r.Key == key).Select(r => r.Value).SingleOrDefault();

                // ReSharper disable once UseObjectOrCollectionInitializer
                StatisticsDto statistics = new StatisticsDto
                {
                    Enqueued = GetValueOrDefault("Enqueued"),
                    Failed = GetValueOrDefault("Failed"),
                    Processing = GetValueOrDefault("Processing"),
                    Scheduled = GetValueOrDefault("Scheduled"),
                    Succeeded = GetValueOrDefault("stats:succeeded"),
                    Deleted = GetValueOrDefault("stats:deleted"),
                    Recurring = GetValueOrDefault("recurring-jobs"),
                    Servers = GetValueOrDefault("Servers"),
                };

                statistics.Queues = storage.QueueProviders
                    .SelectMany(x => x.GetJobQueueMonitoringApi().GetQueues())
                    .Count();

                return statistics;
            }
        }

        #region Job List

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                BucketContext context = new BucketContext(bucket);
                List<Documents.Queue> queues = context.Query<Documents.Queue>()
                    .Where(q => q.DocumentType == DocumentTypes.Queue)
                    .OrderBy(q => q.CreatedOn)
                    .Skip(from).Take(perPage)
                    .AsEnumerable()
                    .Where(q => q.FetchedAt.HasValue == false)
                    .ToList();

                return GetJobsOnQueue(bucket, queues, (state, job, fetchedAt) => new EnqueuedJobDto
                {
                    Job = job,
                    State = state.Name,
                    InEnqueuedState = EnqueuedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                    EnqueuedAt = EnqueuedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase)
                        ? JobHelper.DeserializeNullableDateTime(state.Data["EnqueuedAt"])
                        : null
                });
            }
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                BucketContext context = new BucketContext(bucket);
                List<Documents.Queue> queues = context.Query<Documents.Queue>()
                    .Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue)
                    .OrderBy(q => q.CreatedOn)
                    .Skip(from).Take(perPage)
                    .AsEnumerable()
                    .Where(q => q.FetchedAt.HasValue)
                    .ToList();

                return GetJobsOnQueue(bucket, queues, (state, job, fetchedAt) => new FetchedJobDto
                {
                    Job = job,
                    State = state.Name,
                    FetchedAt = fetchedAt?.ToDateTime()
                });
            }
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return GetJobsOnState(ProcessingState.StateName, from, count, (state, job) => new ProcessingJobDto
            {
                Job = job,
                InProcessingState = ProcessingState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                ServerId = state.Data.ContainsKey("ServerId") ? state.Data["ServerId"] : state.Data["ServerName"],
                StartedAt = JobHelper.DeserializeDateTime(state.Data["StartedAt"])
            });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return GetJobsOnState(ScheduledState.StateName, from, count, (state, job) => new ScheduledJobDto
            {
                Job = job,
                InScheduledState = ScheduledState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                EnqueueAt = JobHelper.DeserializeDateTime(state.Data["EnqueueAt"]),
                ScheduledAt = JobHelper.DeserializeDateTime(state.Data["ScheduledAt"])
            });
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return GetJobsOnState(SucceededState.StateName, from, count, (state, job) => new SucceededJobDto
            {
                Job = job,
                InSucceededState = SucceededState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                Result = state.Data.ContainsKey("Result") ? state.Data["Result"] : null,
                TotalDuration = state.Data.ContainsKey("PerformanceDuration") && state.Data.ContainsKey("Latency")
                                ? (long?)long.Parse(state.Data["PerformanceDuration"]) + long.Parse(state.Data["Latency"])
                                : null,
                SucceededAt = JobHelper.DeserializeNullableDateTime(state.Data["SucceededAt"])
            });
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return GetJobsOnState(FailedState.StateName, from, count, (state, job) => new FailedJobDto
            {
                Job = job,
                InFailedState = FailedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                Reason = state.Reason,
                FailedAt = JobHelper.DeserializeNullableDateTime(state.Data["FailedAt"]),
                ExceptionDetails = state.Data["ExceptionDetails"],
                ExceptionMessage = state.Data["ExceptionMessage"],
                ExceptionType = state.Data["ExceptionType"],
            });
        }

        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return GetJobsOnState(DeletedState.StateName, from, count, (state, job) => new DeletedJobDto
            {
                Job = job,
                InDeletedState = DeletedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                DeletedAt = JobHelper.DeserializeNullableDateTime(state.Data["DeletedAt"])
            });
        }

        private JobList<T> GetJobsOnState<T>(string stateName, int from, int count, Func<State, Common.Job, T> selector)
        {
            List<KeyValuePair<string, T>> jobs = new List<KeyValuePair<string, T>>();

            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                BucketContext context = new BucketContext(bucket);

                List<Documents.Job> filterJobs = context.Query<Documents.Job>()
                    .Where(j => j.DocumentType == DocumentTypes.Job && j.StateName == stateName)
                    .OrderByDescending(j => j.CreatedOn)
                    .Skip(from).Take(count)
                    .ToList();

                filterJobs.ForEach(job =>
                {
                    IDocumentResult<State> result = bucket.GetDocument<State>(job.StateId);
                    if (result.Success && result.Content != null)
                    {
                        InvocationData invocationData = job.InvocationData;
                        invocationData.Arguments = job.Arguments;

                        T data = selector(result.Content, invocationData.Deserialize());
                        jobs.Add(new KeyValuePair<string, T>(job.Id, data));
                    }
                });
            }

            return new JobList<T>(jobs);
        }

        private JobList<T> GetJobsOnQueue<T>(IBucket bucket, List<Documents.Queue> queues, Func<State, Common.Job, int?, T> selector)
        {
            List<KeyValuePair<string, T>> jobs = new List<KeyValuePair<string, T>>();
            queues.ForEach(queueItem =>
            {
                IDocumentResult<Documents.Job> result = bucket.GetDocument<Documents.Job>(queueItem.JobId);
                if (result.Success && result.Content != null)
                {
                    Documents.Job job = result.Content;
                    InvocationData invocationData = job.InvocationData;
                    invocationData.Arguments = job.Arguments;

                    IDocumentResult<State> stateResult = bucket.GetDocument<State>(job.StateId);

                    T data = selector(stateResult.Content, invocationData.Deserialize(), queueItem.FetchedAt);
                    jobs.Add(new KeyValuePair<string, T>(job.Id, data));
                }
            });

            return new JobList<T>(jobs);
        }

        #endregion

        #region Counts

        public long EnqueuedCount(string queue)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));

            IPersistentJobQueueProvider provider = storage.QueueProviders.GetProvider(queue);
            IPersistentJobQueueMonitoringApi monitoringApi = provider.GetJobQueueMonitoringApi();
            (int? EnqueuedCount, int? FetchedCount) counters = monitoringApi.GetEnqueuedAndFetchedCount(queue);
            return counters.EnqueuedCount ?? 0;
        }

        public long FetchedCount(string queue)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));

            IPersistentJobQueueProvider provider = storage.QueueProviders.GetProvider(queue);
            IPersistentJobQueueMonitoringApi monitoringApi = provider.GetJobQueueMonitoringApi();
            (int? EnqueuedCount, int? FetchedCount) counters = monitoringApi.GetEnqueuedAndFetchedCount(queue);
            return counters.FetchedCount ?? 0;
        }

        public long ScheduledCount() => GetNumberOfJobsByStateName(ScheduledState.StateName);

        public long FailedCount() => GetNumberOfJobsByStateName(FailedState.StateName);

        public long ProcessingCount() => GetNumberOfJobsByStateName(ProcessingState.StateName);

        public long SucceededListCount() => GetNumberOfJobsByStateName(SucceededState.StateName);

        public long DeletedListCount() => GetNumberOfJobsByStateName(DeletedState.StateName);

        private long GetNumberOfJobsByStateName(string state)
        {
            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                BucketContext context = new BucketContext(bucket);
                return context.Query<Documents.Job>()
                    .Where(j => j.DocumentType == DocumentTypes.Job && j.StateName == state)
                    .LongCount();
            }
        }

        public IDictionary<DateTime, long> SucceededByDatesCount() => GetDatesTimelineStats("succeeded");

        public IDictionary<DateTime, long> FailedByDatesCount() => GetDatesTimelineStats("failed");

        public IDictionary<DateTime, long> HourlySucceededJobs() => GetHourlyTimelineStats("succeeded");

        public IDictionary<DateTime, long> HourlyFailedJobs() => GetHourlyTimelineStats("failed");

        private Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
        {
            DateTime endDate = DateTime.UtcNow;
            List<DateTime> dates = new List<DateTime>();
            for (int i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            Dictionary<string, DateTime> keys = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd-HH}", x => x);
            return GetTimelineStats(keys);
        }

        private Dictionary<DateTime, long> GetDatesTimelineStats(string type)
        {
            DateTime endDate = DateTime.UtcNow.Date;
            List<DateTime> dates = new List<DateTime>();
            for (int i = 0; i < 7; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            Dictionary<string, DateTime> keys = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd}", x => x);
            return GetTimelineStats(keys);
        }

        private Dictionary<DateTime, long> GetTimelineStats(Dictionary<string, DateTime> keys)
        {
            Dictionary<DateTime, long> result = keys.ToDictionary(k => k.Value, v => default(long));
            Dictionary<string, int> data;

            using (IBucket bucket = storage.Client.OpenBucket(storage.Options.DefaultBucket))
            {
                BucketContext context = new BucketContext(bucket);
                data = context.Query<Counter>()
                    .Where(c => c.Type == CounterTypes.Aggregate && c.DocumentType == DocumentTypes.Counter)
                    .AsEnumerable()
                    .Where(c => keys.ContainsKey(c.Key))
                    .ToDictionary(k => k.Key, k => k.Value);
            }

            foreach (string key in keys.Keys)
            {
                DateTime date = keys.Where(k => k.Key == key).Select(k => k.Value).First();
                result[date] = data.ContainsKey(key) ? data[key] : 0;
            }

            return result;
        }

        #endregion
    }
}
