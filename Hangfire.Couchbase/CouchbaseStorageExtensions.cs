using System;
using Hangfire.Couchbase;

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
        /// <param name="configuration">The IGlobalConfiguration object</param>
        /// <param name="configurationSectionName">The configuration section name</param>
        /// <param name="bucket">The name of the bucket to connect with</param>
        /// <returns></returns>
        public static IGlobalConfiguration<CouchbaseStorage> UseAzureDocumentDbStorage(this IGlobalConfiguration configuration, string configurationSectionName, string bucket = "default", CouchbaseStorageOptions options = null)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (string.IsNullOrEmpty(configurationSectionName)) throw new ArgumentNullException(nameof(configurationSectionName));
            if (string.IsNullOrEmpty(bucket)) throw new ArgumentNullException(nameof(bucket));

            CouchbaseStorage storage = new CouchbaseStorage(configurationSectionName, bucket, options);
            return configuration.UseStorage(storage);
        }
    }
}
