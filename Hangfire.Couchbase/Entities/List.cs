// ReSharper disable CheckNamespace
using Newtonsoft.Json;

namespace Hangfire.Couchbase.Documents
{
    internal class List : DocumentBase
    {
        [JsonProperty("key")]
        public string Key { get; set; }

        [JsonProperty("value")]
        public string Value { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.List;
    }
}
