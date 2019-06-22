using System;
using System.Linq;

using Couchbase.Management;
using Couchbase.Management.Indexes;
using Hangfire.Couchbase.Exceptions;

namespace Hangfire.Couchbase.Extension
{
    internal static class BucketManagerExtension
    {
        /// <summary>
        /// Checks if all required indexes exists for the bucket
        /// </summary>
        /// <param name="indexes">Array of index names</param>
        /// <exception>Throw the exception, if any of the indexes are not present
        ///     <cref>Hangfire.Couchbase.Exceptions.HangfireCouchbaseMissingIndexesException</cref>
        /// </exception>
        // ReSharper disable once InvalidXmlDocComment
        internal static void CheckIndexes(this IBucketManager manager, params string[] indexes)
        {
            // get all the indexes 
            IndexResult indexList = manager.ListN1qlIndexes();

            // check if all the indexes are created
            bool hasIndexes = indexes.All(i => indexList.Any(idx => string.Equals(idx.Name, i, StringComparison.InvariantCultureIgnoreCase)));
            if (!hasIndexes)
            {
                throw new HangfireCouchbaseMissingIndexesException($"User does not have credentials to run index operations. Add role query_manage_index on {manager.BucketName} to allow the query to run.");
            }
        }
    }
}
