# Hangfire.Couchbase

[![Official Site](https://img.shields.io/badge/site-hangfire.io-blue.svg)](http://hangfire.io)
[![Latest version](https://img.shields.io/nuget/v/Hangfire.Couchbase.svg)](https://www.nuget.org/packages/Hangfire.Couchbase)
[![Build status](https://ci.appveyor.com/api/projects/status/4rkyu51n3ybdguiu?svg=true)](https://ci.appveyor.com/project/imranmomin/hangfire-couchbase)

This repo will add a [Couchbase](https://www.couchbase.com/products/server) N1QL storage support to [Hangfire](http://hangfire.io) - fire-and-forget, delayed and recurring tasks runner for .NET. Scalable and reliable background job runner. Supports multiple servers, CPU and I/O intensive, long-running and short-running jobs.

## Installation

[Hangfire.Couchbase](https://www.nuget.org/packages/Hangfire.Couchbase) is available as a NuGet package. Install it using the NuGet Package Console window:

```powershell
PM> Install-Package Hangfire.Couchbase
```

## Usage

##### 1. Using the `ClientConfiguration` to initialize the server

```csharp
ClientConfiguration configuration = new ClientConfiguration {
    BucketConfigs = new Dictionary<string, BucketConfiguration> {
      {"default", new BucketConfiguration {
          PoolConfiguration = new PoolConfiguration {
              MaxSize = 6,
              MinSize = 4,
              SendTimeout = 12000
          },
          DefaultOperationLifespan = 123,
          Password = "password",
          Username = "username",
          BucketName = "default"
      }}
    }
};

GlobalConfiguration.Configuration.UseCouchbaseStorage(configuration, "<defaultBucket>");
```

If you're using Couchbase 5.0+, then you'll need to supply RBAC credentials:
 ```csharp
ClientConfiguration configuration = new ClientConfiguration {
    BucketConfigs = new Dictionary<string, BucketConfiguration> {
      {"default", new BucketConfiguration {
          PoolConfiguration = new PoolConfiguration {
              MaxSize = 6,
              MinSize = 4,
              SendTimeout = 12000
          },
          DefaultOperationLifespan = 123,
          BucketName = "default"
      }}
    }
};
 configuration.SetAuthenticator(new PasswordAuthenticator("<username>", "<password>"));
 GlobalConfiguration.Configuration.UseCouchbaseStorage(configuration, "<defaultBucket>");
```

##### 2. Using the XML configuration to intialzie .Net application

```xml
<configSections>
  <sectionGroup name="couchbaseClients">
    <section name="couchbase" type="Couchbase.Configuration.Client.Providers.CouchbaseClientSection, Couchbase.NetClient" />
  </sectionGroup>
</configSections>
<couchbaseClients>
  <couchbase>
    <servers>
      <add uri="http://localhost:8091"></add>
    </servers>
    <buckets>
      <add name="default" password="password" />
    </buckets>
    <serializer name="CustomSerializer" type="Hangfire.Couchbase.Json.DocumentDefaultSerializer, Hangfire.Couchbase" />
  </couchbase>
</couchbaseClients>
```

```csharp
GlobalConfiguration.Configuration.UseCouchbaseStorage("couchbaseClients/couchbase", "<defaultBucket>");
```

##### 3. Using the JSON configuration for .Net Core application

```json
{
  "couchbase": {
    "basic": {
      "servers": [
        "http://localhost:8091/"
      ],
      "buckets": [
        {
          "name": "default",
          "password": "password"
        }
      ],
      "serializer": "Hangfire.Couchbase.Json.DocumentDefaultSerializer, Hangfire.Couchbase"
    }
  }
}
```

```csharp
IConfigurationBuilder builder = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

IConfigurationRoot configuration = builder.Build();
GlobalConfiguration.Configuration.UseCouchbaseStorage(configuration, "couchbase:basic", "<defaultBucket>");
```

**You can also customize some of the options to fine tune the job server. The below are defaults; if no overrides found**

```csharp
Hangfire.Couchbase.CouchbaseStorageOptions options = new Hangfire.Couchbase.CouchbaseStorageOptions
{
    RequestTimeout = TimeSpan.FromSeconds(30),
    ExpirationCheckInterval = TimeSpan.FromSeconds(30),
    CountersAggregateInterval = TimeSpan.FromMinutes(1),
    QueuePollInterval = TimeSpan.FromSeconds(2)
};

GlobalConfiguration.Configuration.UseCouchbaseStorage(configuration, "<defaultBucket>", options);
```

## Questions? Problems?

Open-source project are developing more smoothly, when all discussions are held in public.

If you have any questions or problems related to Hangfire.Couchbase itself or this storage implementation or want to discuss new features, please create under [issues](https://github.com/imranmomin/Hangfire.Couchbase/issues/new) and assign the correct label for discussion. 

If you've discovered a bug, please report it to the [GitHub Issues](https://github.com/imranmomin/Hangfire.Couchbase/pulls). Detailed reports with stack traces, actual and expected behavours are welcome.