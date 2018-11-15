using System;
using Hangfire.Storage;

namespace Hangfire.Couchbase.RabbitMq
{
    internal class RabbitMqFetchedJob : IFetchedJob
    {
        public string JobId { get; }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public void RemoveFromQueue()
        {
            throw new NotImplementedException();
        }

        public void Requeue()
        {
            throw new NotImplementedException();
        }

    }
}
