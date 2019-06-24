using System;
using Hangfire.Couchbase;
using Couchbase.Configuration.Client;

#if NETSTANDARD
using Microsoft.Extensions.Configuration;
#endif

// ReSharper disable UnusedMember.Global
// ReSharper disable once CheckNamespace
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
        /// <param name="options">The CouchbaseStorageOptions object to override any of the options</param>
        /// <returns></returns>
        public static IGlobalConfiguration<CouchbaseStorage> UseCouchbaseStorage(this IGlobalConfiguration global, ClientConfiguration configuration, string defaultBucket = "default", CouchbaseStorageOptions options = null)
        {
            if (global == null) throw new ArgumentNullException(nameof(global));
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (string.IsNullOrEmpty(defaultBucket)) throw new ArgumentNullException(nameof(defaultBucket));

            CouchbaseStorage storage = new CouchbaseStorage(configuration, defaultBucket, options);
            return global.UseStorage(storage);
        }

#if NETFULL
        /// <summary>
        /// Enables to attach Couchbase to Hangfire
        /// </summary>
        /// <param name="global">The IGlobalConfiguration object</param>
        /// <param name="sectionName">The xml configuration section name</param>
        /// <param name="defaultBucket">The default name of the bucket to use</param>
        /// <param name="options">The CouchbaseStorageOptions object to override any of the options</param>
        /// <returns></returns>
        public static IGlobalConfiguration<CouchbaseStorage> UseCouchbaseStorage(this IGlobalConfiguration global, string sectionName, string defaultBucket = "default", CouchbaseStorageOptions options = null)
        {
            if (global == null) throw new ArgumentNullException(nameof(global));
            if (string.IsNullOrEmpty(sectionName)) throw new ArgumentNullException(nameof(sectionName));
            if (string.IsNullOrEmpty(defaultBucket)) throw new ArgumentNullException(nameof(defaultBucket));

            CouchbaseStorage storage = new CouchbaseStorage(sectionName, defaultBucket, options);
            return global.UseStorage(storage);
        }
#endif

#if NETSTANDARD
        /// <summary>
        /// Enables to attach Couchbase to Hangfire
        /// </summary>
        /// <param name="global">The IGlobalConfiguration object</param>
        /// <param name="configuration">Represents a set of key/value application configuration properties.</param>
        /// <param name="sectionName">The configuration section name</param>
        /// <param name="defaultBucket">The default name of the bucket to use</param>
        /// <param name="options">The CouchbaseStorageOptions object to override any of the options</param>
        /// <returns></returns>
        public static IGlobalConfiguration<CouchbaseStorage> UseCouchbaseStorage(this IGlobalConfiguration global, IConfiguration configuration, string sectionName, string defaultBucket = "default", CouchbaseStorageOptions options = null)
        {
            if (global == null) throw new ArgumentNullException(nameof(global));
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (string.IsNullOrEmpty(sectionName)) throw new ArgumentNullException(nameof(sectionName));
            if (string.IsNullOrEmpty(defaultBucket)) throw new ArgumentNullException(nameof(defaultBucket));

            CouchbaseStorage storage = new CouchbaseStorage(configuration, sectionName, defaultBucket, options);
            return global.UseStorage(storage);
        }
#endif

    }
}
