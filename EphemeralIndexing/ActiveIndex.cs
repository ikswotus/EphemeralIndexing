using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EphemeralIndexing
{
    /// <summary>
    /// Simple intermediate class to pair up indexes, chunks, and hypertables
    /// </summary>
    public class ActiveIndex
    {
        /// <summary>
        /// Name of an individual chunk (
        /// </summary>
        public string ChunkName { get; set; }
        public string TableName { get; set; }
        public string IndexName { get; set; }
    }
}
