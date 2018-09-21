// ReSharper disable CheckNamespace

using System;

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Hangfire.Couchbase.Documents
{
    internal class Server : DocumentBase
    {
        [JsonProperty("server_id")]
        public string ServerId { get; set; }

        [JsonProperty("workers")]
        public int Workers { get; set; }

        [JsonProperty("queues")]
        public string[] Queues { get; set; }

        [JsonProperty("created_on")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime CreatedOn { get; set; }

        [JsonProperty("last_heartbeat")]
        public int LastHeartbeat { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.Server;
    }
}