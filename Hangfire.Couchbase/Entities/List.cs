// ReSharper disable CheckNamespace

using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Hangfire.Couchbase.Documents
{
    internal class List : DocumentBase
    {
        [JsonProperty("key")]
        public string Key { get; set; }

        [JsonProperty("value")]
        public string Value { get; set; }

        [JsonProperty("created_on")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime CreatedOn { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.List;

        public List() : base("hangifre:list") { }
    }
}
