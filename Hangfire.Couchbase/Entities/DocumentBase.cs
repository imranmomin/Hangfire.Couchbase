// ReSharper disable CheckNamespace
using System;
using Newtonsoft.Json;

namespace Hangfire.Couchbase.Documents
{
    internal abstract class DocumentBase
    {
        [JsonProperty("id")]
        public string Id { get; set; } = Guid.NewGuid().ToString();

        [JsonProperty("expire_on")]
        public int? ExpireOn { get; set; }

        [JsonProperty("type")]
        public abstract DocumentTypes DocumentType { get; }
    }

    internal enum DocumentTypes
    {
        Server = 1,
        Job = 2,
        Queue = 3,
        Counter = 4,
        List = 5,
        Hash = 6,
        Set = 7,
        State = 8,
        Lock = 9
    }
}
