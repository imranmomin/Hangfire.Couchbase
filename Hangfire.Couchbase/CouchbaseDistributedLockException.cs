using System;

namespace Hangfire.Couchbase
{
    /// <summary>
    /// Represents errors that occur while acquiring a distributed lock.
    /// </summary>
    [Serializable]
    public class CouchbaseDistributedLockException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the CouchbaseDistributedLockException class with serialized data.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public CouchbaseDistributedLockException(string message) : base(message)
        {
        }
    }
}
