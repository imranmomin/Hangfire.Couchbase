// ReSharper disable CheckNamespace
using Newtonsoft.Json;

namespace Hangfire.Couchbase.Documents
{
    internal class Lock : DocumentBase
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.Lock;

        public Lock() : base("hangifre::lock") { }
    }
}
