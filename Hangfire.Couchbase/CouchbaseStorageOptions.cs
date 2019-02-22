using System;

// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global

namespace Hangfire.Couchbase
{
    /// <summary>
    /// Options for CouchbaseStorage
    /// </summary>
    public class CouchbaseStorageOptions
    {
        internal string DefaultBucket { get; set; }

        /// <summary>
        /// Get or sets the request timeout for Couchbase client. Default value set to 30 seconds
        /// </summary>
        public TimeSpan RequestTimeout { get; set; }

        /// <summary>
        /// Get or set the interval timespan to process expired entries. Default value 2 minutes
        /// Expired items under "locks", "jobs", "lists", "sets", "hashes", "counters/aggregated" will be checked 
        /// </summary>
        public TimeSpan ExpirationCheckInterval { get; set; }

        /// <summary>
        /// Get or sets the interval timespan to aggregated the counters. Default value 2 minute
        /// </summary>
        public TimeSpan CountersAggregateInterval { get; set; }

        /// <summary>
        /// Gets or sets the interval timespan to poll the queue for processing any new jobs. Default value 15 seconds
        /// </summary>
        public TimeSpan QueuePollInterval { get; set; }

        /// <summary>
        /// Gets or sets the status whether to create primary index on the bucket;. Default true
        /// </summary>
        public bool CreatePrimaryKeyIndex { get; set; }

        /// <summary>
        /// Create an instance of Couchbase Storage option with default values
        /// </summary>
        public CouchbaseStorageOptions()
        {
            RequestTimeout = TimeSpan.FromSeconds(30);
            ExpirationCheckInterval = TimeSpan.FromMinutes(2);
            CountersAggregateInterval = TimeSpan.FromMinutes(2);
            QueuePollInterval = TimeSpan.FromSeconds(15);
            CreatePrimaryKeyIndex = true;
        }
    }
}
