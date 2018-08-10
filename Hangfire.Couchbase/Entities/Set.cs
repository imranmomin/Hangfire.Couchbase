﻿// ReSharper disable CheckNamespace

using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

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

        [JsonProperty("created_on")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime CreatedOn { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.Set;

        public Set() : base("hangifre::set") { }
    }
}