// ReSharper disable CheckNamespace
using Newtonsoft.Json;

namespace Hangfire.Couchbase.Documents
{
    internal class Parameter
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("value")]
        public string Value { get; set; }
    }
}