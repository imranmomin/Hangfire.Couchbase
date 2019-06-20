using System;
using System.Linq;
using Couchbase;
using Couchbase.Management;

namespace Hangfire.Couchbase
{
    public static class BucketManagerExtensions
    {
        public static IResult CreateAndCheckN1qlPrimaryIndex(this IBucketManager @this, string indexName)
        {
            var result = @this.CreateN1qlPrimaryIndex(indexName, false);
            AndCheckN1QlIndex(@this, indexName, result);
            return result;
        }

        public static IResult CreateAndCheckN1qlIndex(this IBucketManager @this, string indexName, params string[] fields)
        {
            var result = @this.CreateN1qlIndex(indexName, false, fields);
            AndCheckN1QlIndex(@this, indexName, result);
            return result;
        }

        private static void AndCheckN1QlIndex(IBucketManager @this, string indexName, IResult result)
        {
            var indexes = @this.ListN1qlIndexes();
            var doesIndexExist = indexes.Any(i => i.Name == indexName);
            if (!doesIndexExist)
            {
                if (result.Exception != null)
                    throw result.Exception;
                throw new Exception(result.ToString());
            }
        }
    }
}