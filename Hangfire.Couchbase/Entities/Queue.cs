// ReSharper disable CheckNamespace

using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Hangfire.Couchbase.Documents
{
    class Queue : DocumentBase
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("job_id")]
        public ulong JobId { get; set; }

        [JsonProperty("created_on")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime CreatedOn { get; set; }

        [JsonProperty("fetched_at")]
        public int? FetchedAt { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.Queue;
    }
}
