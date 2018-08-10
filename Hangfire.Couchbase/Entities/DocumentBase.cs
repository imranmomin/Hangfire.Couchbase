// ReSharper disable CheckNamespace
using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Hangfire.Couchbase.Documents
{
    internal abstract class DocumentBase
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("expire_on")]
        public int? ExpireOn { get; set; }

        [JsonProperty("type")]
        public abstract DocumentTypes DocumentType { get; }

        protected DocumentBase(string prefix)
        {
            Id = $"{prefix}::{Guid.NewGuid().ToString()}";
        }
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
