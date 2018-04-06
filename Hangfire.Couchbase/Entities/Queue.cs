// ReSharper disable CheckNamespace
using Newtonsoft.Json;

namespace Hangfire.Couchbase.Documents
{
    class Queue : DocumentBase
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("job_id")]
        public string JobId { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.Queue;
    }
}
