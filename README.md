# Hangfire.Couchbase

[![Official Site](https://img.shields.io/badge/site-hangfire.io-blue.svg)](http://hangfire.io)
[![Latest version](https://img.shields.io/nuget/v/Hangfire.Couchbase.svg)](https://www.nuget.org/packages/Hangfire.Couchbase)
[![Build status](https://ci.appveyor.com/api/projects/status/uvxh94dhxcokga47?svg=true)](https://ci.appveyor.com/project/imranmomin/hangfire-couchbase)

This repo will add a [Couchbase](https://www.couchbase.com/products/server) N1QL storage support to [Hangfire](http://hangfire.io) - fire-and-forget, delayed and recurring tasks runner for .NET. Scalable and reliable background job runner. Supports multiple servers, CPU and I/O intensive, long-running and short-running jobs.


## Installation

[Hangfire.Couchbase](https://www.nuget.org/packages/Hangfire.Couchbase) is available as a NuGet package. Install it using the NuGet Package Console window:

```powershell
PM> Install-Package Hangfire.Couchbase
```


## Usage

Use one the following ways to initialize `CouchbaseStorage`

```csharp


GlobalConfiguration.Configuration.UseCouchbaseStorage(configuration, "<defaultBucket>");

Hangfire.Couchbase.CouchbaseStorage storage = new Hangfire.Couchbase.CouchbaseStorage(configuration", "<defaultBucket>");
GlobalConfiguration.Configuration.UseStorage(storage);
```

```csharp
// customize any options
Hangfire.Couchbase.CouchbaseStorageOptions options = new Hangfire.Couchbase.CouchbaseStorageOptions
{
    RequestTimeout = TimeSpan.FromSeconds(30),
    ExpirationCheckInterval = TimeSpan.FromMinutes(15),
    CountersAggregateInterval = TimeSpan.FromMinutes(1),
    QueuePollInterval = TimeSpan.FromSeconds(2)
};

GlobalConfiguration.Configuration.UseCouchbaseStorage(configuration, "<defaultBucket>", options);

// or 

Hangfire.Couchbase.CouchbaseStorage storage = new Hangfire.Couchbase.CouchbaseStorage(configuration, "<defaultBucket>", options);
GlobalConfiguration.Configuration.UseStorage(storage);
```


## Questions? Problems?

Open-source project are developing more smoothly, when all discussions are held in public.

If you have any questions or problems related to Hangfire.Couchbase itself or this storage implementation or want to discuss new features, please create under [issues](https://github.com/imranmomin/Hangfire.Couchbase/issues/new) and assign the correct label for discussion. 

If you've discovered a bug, please report it to the [GitHub Issues](https://github.com/imranmomin/Hangfire.Couchbase/pulls). Detailed reports with stack traces, actual and expected behavours are welcome.