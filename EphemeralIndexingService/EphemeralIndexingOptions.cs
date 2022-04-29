using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Xml;
using System.Xml.Serialization;
using System.IO;

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
        ///  Partial index name used to identify a set of indexes
        /// </summary>
        public string IndexName { get; set; }

        /// <summary>
        ///  Columns for index, comma-separated
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

    public class ConfiguredOptions
    {
        public ConfiguredOptions()
        {
            Options = new List<EphemeralIndexingOptions>();
            ConnectionString = String.Empty;
        }

        public string ConnectionString { get; set; }

        public List<EphemeralIndexingOptions> Options { get; set; }
    }


    public static class OptionsHelper
    {
        public static XmlSerializer _xs = new XmlSerializer(typeof(ConfiguredOptions));

        public static ConfiguredOptions FromFile(string file)
        {
            using (FileStream fs = File.OpenRead(file))
            {
                return (ConfiguredOptions)_xs.Deserialize(fs);
            }
        }
        public static void ToFile(ConfiguredOptions options, string file)
        {
            using (FileStream fs = File.Create(file))
            {
                _xs.Serialize(fs, options);
            }
        }
    }
}
