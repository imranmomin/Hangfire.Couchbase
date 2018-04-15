// ReSharper disable CheckNamespace

using Newtonsoft.Json;

namespace Hangfire.Couchbase.Documents
{
    internal class Set : DocumentBase
    {
        [JsonProperty("key")]
        public string Key { get; set; }

        [JsonProperty("value")]
        public string Value { get; set; }

        [JsonProperty("score")]
        public double? Score { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.Set;
    }
}