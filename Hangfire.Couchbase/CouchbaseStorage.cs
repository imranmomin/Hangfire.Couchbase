using System;
using System.Linq;
using System.Collections.Generic;

using Couchbase;
using Couchbase.Core;
using Hangfire.Server;
using Hangfire.Storage;
using Hangfire.Logging;
using Couchbase.Management;
using Couchbase.Configuration.Client;

using Hangfire.Couchbase.Json;
using Hangfire.Couchbase.Queue;
using Hangfire.Couchbase.Extension;

#if NETFULL
using Couchbase.Configuration.Client.Providers;
#endif

#if NETSTANDARD
using Microsoft.Extensions.Configuration;
#endif

namespace Hangfire.Couchbase
{
    /// <summary>
    /// CouchbaseStorage extend the storage option for Hangfire.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    public sealed class CouchbaseStorage : JobStorage
    {
        internal CouchbaseStorageOptions Options { get; private set; }

        internal PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        internal Cluster Client { get; private set; }

        /// <summary>
        /// Initializes the CouchbaseStorage form ClientConfiguration.
        /// </summary>
        /// <param name="configuration">The configuration</param>
        /// <param name="defaultBucket">The default name of the bucket to use</param>
        /// <param name="options">The CouchbaseStorageOptions object to override any of the options</param>
        public CouchbaseStorage(ClientConfiguration configuration, string defaultBucket = "default", CouchbaseStorageOptions options = null)
        {
            Initialize(configuration, defaultBucket, options);
        }

#if NETFULL
        /// <summary>
        /// Initializes the CouchbaseStorage from the XML configuration section.
        /// </summary>
        /// <param name="sectionName">The xml configuration section name</param>
        /// <param name="defaultBucket">The default name of the bucket to use</param>
        /// <param name="options">The CouchbaseStorageOptions object to override any of the options</param>
        public CouchbaseStorage(string sectionName, string defaultBucket = "default", CouchbaseStorageOptions options = null)
        {
            if (string.IsNullOrEmpty(sectionName)) throw new ArgumentNullException(nameof(sectionName));

            CouchbaseClientSection configurationSection = System.Configuration.ConfigurationManager.GetSection(sectionName) as CouchbaseClientSection;
            ClientConfiguration configuration = new ClientConfiguration(configurationSection);
            Initialize(configuration, defaultBucket, options);
        }
#endif

#if NETSTANDARD
        /// <summary>
        /// Initializes the CouchbaseStorage form the IConfiguration section.
        /// </summary>
        /// <param name="configuration">Represents a set of key/value application configuration properties.</param>
        /// <param name="sectionName">The configuration section name</param>
        /// <param name="defaultBucket">The default name of the bucket to use</param>
        /// <param name="options">The CouchbaseStorageOptions object to override any of the options</param>
        public CouchbaseStorage(IConfiguration configuration, string sectionName, string defaultBucket = "default", CouchbaseStorageOptions options = null)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (string.IsNullOrEmpty(sectionName)) throw new ArgumentNullException(nameof(sectionName));
            
            CouchbaseClientDefinition definition = new CouchbaseClientDefinition();
            configuration.GetSection(sectionName).Bind(definition);
            ClientConfiguration config = new ClientConfiguration(definition);
            Initialize(config, defaultBucket, options);
        }
#endif

        private void Initialize(ClientConfiguration configuration, string defaultBucket, CouchbaseStorageOptions options)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (string.IsNullOrEmpty(defaultBucket)) throw new ArgumentNullException(nameof(defaultBucket));

            Options = options ?? new CouchbaseStorageOptions();
            Options.DefaultBucket = defaultBucket;

            configuration.Serializer = () => new DocumentDefaultSerializer();
            Client = new Cluster(configuration);

            if (Options.CreateDefaultIndexes)
            {
                string indexPrefix = $"IDX_{defaultBucket}";

                // index definition
                (string name, bool isPrimary, string[] fields)[] indexDefinition =
                {
                ($"{indexPrefix}_Primary", true, null),
                ($"{indexPrefix}_Type", false, new [] { "type" }),
                ($"{indexPrefix}_Id", false, new[] { "id" }),
                ($"{indexPrefix}_Expire", false, new[] { "expire_on" }),
                ($"{indexPrefix}_Name", false, new[] { "name" })
            };

                IBucket bucket = Client.OpenBucket(Options.DefaultBucket);
                {
                    IBucketManager manager = bucket.CreateManager(bucket.Configuration.Username, bucket.Configuration.Password);

                    // create all the indexes
                    foreach ((string name, bool isPrimary, string[] fields) index in indexDefinition)
                    {
                        if (index.isPrimary && Options.CreatePrimaryIndex) manager.CreateN1qlPrimaryIndex(index.name, false);
                        else manager.CreateN1qlIndex(index.name, false, index.fields);
                    }

                    // check if all the required indexes are created
                    string[] indexes = indexDefinition.Select(i => i.name).ToArray();
                    manager.CheckIndexes(indexes); // will throw exception if any of the indexes don't exists
                }
            }

            JobQueueProvider provider = new JobQueueProvider(this);
            QueueProviders = new PersistentJobQueueProviderCollection(provider);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IStorageConnection GetConnection() => new CouchbaseConnection(this);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IMonitoringApi GetMonitoringApi() => new CouchbaseMonitoringApi(this);

#pragma warning disable 618
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore 618
        {
            yield return new ExpirationManager(this);
        }

        /// <summary>
        /// Prints out the storage options
        /// </summary>
        /// <param name="logger"></param>
        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Couchbase job storage:");
            logger.Info($"     Couchbase Url: {string.Join(",", Client.Configuration.Servers.Select(s => s.AbsoluteUri))}");
            logger.Info($"     Request Timeout: {Options.RequestTimeout}");
            logger.Info($"     Counter Aggregate Interval: {Options.CountersAggregateInterval.TotalSeconds} seconds");
            logger.Info($"     Queue Poll Interval: {Options.QueuePollInterval.TotalSeconds} seconds");
            logger.Info($"     Expiration Check Interval: {Options.ExpirationCheckInterval.TotalSeconds} seconds");
        }

        /// <summary>
        /// Return the name of the database
        /// </summary>
        /// <returns></returns>
        public override string ToString() => $"Bucket : {Options.DefaultBucket}";
    }
}
