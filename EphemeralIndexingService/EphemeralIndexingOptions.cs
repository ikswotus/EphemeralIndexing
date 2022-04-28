using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EphemeralIndexingService
{
    /// <summary>
    /// Configurable options for setting up indexing on a hypertable
    /// </summary>
    public class EphemeralIndexingOptions
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public EphemeralIndexingOptions()
        {
            Enabled = true;
            AgeToIndex = TimeSpan.FromHours(12);
            Predicate = null;
        }

        /// <summary>
        /// True if this set of indexing options is enabled
        /// </summary>
        public bool Enabled { get; set; }

        /// <summary>
        /// Partial index name used to identify a set of indexes
        /// </summary>
        public string IndexName { get; set; }

        /// <summary>
        /// Columns for index
        /// </summary>
        public string IndexCriteria { get; set; }

        /// <summary>
        /// Time window of chunks that should be indexed.
        /// </summary>
        public TimeSpan AgeToIndex { get; set; }

        /// <summary>
        /// Time column for the hypertable
        /// </summary>
        public string TimeColumn { get; set; }

        /// <summary>
        /// Table to index
        /// </summary>
        public string Hypertable { get; set; }

        /// <summary>
        /// Optional - where predicate for partial indexes
        /// </summary>
        public string Predicate { get; set; }
    }
}
