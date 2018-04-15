// ReSharper disable once CheckNamespace
namespace Hangfire.Couchbase.Queue
{
    internal interface IPersistentJobQueueProvider
    {
        IPersistentJobQueue GetJobQueue();
        IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi();
    }
}