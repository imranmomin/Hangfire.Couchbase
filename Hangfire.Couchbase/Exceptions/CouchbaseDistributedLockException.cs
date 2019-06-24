using System;

namespace Hangfire.Couchbase.Exceptions
{
    /// <summary>
    /// Represents errors that occur while acquiring a distributed lock.
    /// </summary>
    [Serializable]
    public class CouchbaseDistributedLockException : Exception
    {
        /// <summary>Initializes a new instance of the <see cref="T:Hangfire.Couchbase.Exceptions.CouchbaseDistributedLockException" /> class with a specific message that describes the current exception.</summary>
        /// <param name="message">A message that describes the current exception.</param>
        public CouchbaseDistributedLockException(string message) : base(message)
        {
        }
    }
}
