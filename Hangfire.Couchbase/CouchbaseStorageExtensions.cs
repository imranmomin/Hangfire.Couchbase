using System;
using Hangfire.Couchbase;
using Couchbase.Configuration.Client;

// ReSharper disable UnusedMember.Global
namespace Hangfire
{
    /// <summary>
    /// Extension methods to user Couchbase Storage.
    /// </summary>
    public static class CouchbaseStorageExtensions
    {
        /// <summary>
        /// Enables to attach Couchbase to Hangfire
        /// </summary>
        /// <param name="global">The IGlobalConfiguration object</param>
        /// <param name="configuration">The configuration</param>
        /// <param name="defaultBucket">The default name of the bucket to use</param>
        /// <param name="options">The CoubaseStorageOptions object to override any of the options</param>
        /// <returns></returns>
        public static IGlobalConfiguration<CouchbaseStorage> UseCouchbaseStorage(this IGlobalConfiguration global, ClientConfiguration configuration, string defaultBucket = "default", CouchbaseStorageOptions options = null)
        {
            if (global == null) throw new ArgumentNullException(nameof(global));
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (string.IsNullOrEmpty(defaultBucket)) throw new ArgumentNullException(nameof(defaultBucket));

            CouchbaseStorage storage = new CouchbaseStorage(configuration, defaultBucket, options);
            return global.UseStorage(storage);
        }
    }
}
