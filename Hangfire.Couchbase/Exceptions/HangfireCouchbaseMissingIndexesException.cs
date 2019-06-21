using System;

namespace Hangfire.Couchbase.Exceptions
{
    internal class HangfireCouchbaseMissingIndexesException : System.Exception
    {
        /// <summary>Initializes a new instance of the <see cref="T:Hangfire.Couchbase.Exceptions.HangfireCouchbaseMissingIndexesException" /> class with a specific message that describes the current exception.</summary>
        /// <param name="message">A message that describes the current exception.</param>
        public HangfireCouchbaseMissingIndexesException(string message) : base(message, null)
        {
        }
    }
}