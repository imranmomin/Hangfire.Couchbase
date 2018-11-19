using Couchbase.Core.Serialization;

using Newtonsoft.Json;
using Hangfire.Couchbase.Documents.Json;

namespace Hangfire.Couchbase.Json
{
    /// <summary>
    /// Default document serializer for the Hangfire Couchbase
    /// </summary>
    public class DocumentDefaultSerializer : DefaultSerializer
    {
        private static readonly JsonSerializerSettings settings = new JsonSerializerSettings
        {
            NullValueHandling = NullValueHandling.Ignore,
            DateTimeZoneHandling = DateTimeZoneHandling.Utc,
            ContractResolver = new DocumentContractResolver()
        };

        public DocumentDefaultSerializer() : base(settings, settings) { }
    }
}
