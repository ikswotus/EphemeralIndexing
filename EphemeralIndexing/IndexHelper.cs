using System;
using System.Linq;
using System.Collections.Generic;

namespace EphemeralIndexing
{
    /// <summary>
    /// Contains methods for working with dynamic(ephemeral) indexes.
    /// </summary>
    public static class IndexHelper
    {
        /// <summary>
        /// Retrieve all chunks for @table
        /// </summary>
        /// <param name="connectionString">DB Connection string</param>
        /// <param name="table">Name of table to retrieve chunks for</param>
        /// <returns>List of all chunks tables</returns>
        public static List<string> GetChunks(string connectionString, string table)
        {
            if (string.IsNullOrEmpty(table))
                throw new ArgumentNullException(nameof(table));

            List<string> chunks = new List<string>();

            using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(connectionString))
            {
                conn.Open();

                string sc = String.Format(ShowChunksFormats, table);

                Npgsql.NpgsqlCommand comm = new Npgsql.NpgsqlCommand(sc, conn);
                using (Npgsql.NpgsqlDataReader reader = comm.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        chunks.Add(reader.GetString(0));
                    }
                }
            }
            return chunks;
        }

        /// <summary>
        /// Retrieve a list of table-index pairings
        /// Item1 = full tablename '_timescaledb_internal.hyper_1_2_chunk'
        /// Item2 = IndexName 'ephemeral_hyper...idx'
        /// </summary>
        /// <param name="connectionString">DB Connection string</param>
        /// <returns></returns>
        public static List<Tuple<string, string>> GetAllEphemeralIndexes(string connectionString)
        {
            List<Tuple<string, string>> ret = new List<Tuple<string, string>>();

            using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(connectionString))
            {
                conn.Open();

                Npgsql.NpgsqlCommand comm = new Npgsql.NpgsqlCommand(GetEphemeralIndexes, conn);
                using (Npgsql.NpgsqlDataReader reader = comm.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ret.Add(new Tuple<string, string>(reader.GetString(0), reader.GetString(1)));
                    }
                }

                return ret;
            }
        }

        /// <summary>
        /// Retrieve the latest timestamp for a table
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="tableName"></param>
        /// <param name="timeColumn"></param>
        /// <returns></returns>
        public static DateTime GetNewestDate(string connectionString, string tableName, string timeColumn)
        {
            string getDate = String.Format(SelectNewestValueFormat, timeColumn, tableName, timeColumn);
            using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(connectionString))
            {
                conn.Open();

                Npgsql.NpgsqlCommand comm = new Npgsql.NpgsqlCommand(getDate, conn);
                object result = comm.ExecuteScalar();
                DateTime dt = result != null ? (DateTime)result : DateTime.MinValue;
                return dt;
            }
        }

        /// <summary>
        /// Returns a mapping of timescale chunks to the main table
        /// </summary>
        /// <param name="connectionString">DB connection string</param>
        /// <returns>Pairing of chunkName to tableName</returns>
        public static Dictionary<string, string> ChunkToRegular(string connectionString)
        {
            Dictionary<string, string> map = new Dictionary<string, string>();

            using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(connectionString))
            {
                conn.Open();

                Npgsql.NpgsqlCommand comm = new Npgsql.NpgsqlCommand(GetAllChunks, conn);
                using (Npgsql.NpgsqlDataReader reader = comm.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        map[reader.GetString(1)] = reader.GetString(0);
                    }
                }
            }

            return map;
        }

        /// <summary>
        /// Returns a mapping of timescale chunks to the main table
        /// </summary>
        /// <param name="connectionString">DB connection string</param>
        /// <param name="hypertables">Tables to limit by</param>
        /// <returns>Pairing of chunkName to tableName</returns>
        public static Dictionary<string, string> ChunkToRegularLimited(string connectionString, IEnumerable<String> hypertables)
        {
            Dictionary<string, string> map = new Dictionary<string, string>();

            using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(connectionString))
            {
                conn.Open();

                string limitedChunks = String.Format(GetAllChunksLimited, String.Join(',', hypertables.Select(s => '\'' + s + '\'')));

                Npgsql.NpgsqlCommand comm = new Npgsql.NpgsqlCommand(limitedChunks, conn);
                using (Npgsql.NpgsqlDataReader reader = comm.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        map[reader.GetString(1)] = reader.GetString(0);
                    }
                }
            }

            return map;
        }

        /// <summary>
        /// Drops an index
        /// </summary>
        /// <param name="connectionString">DB connection string</param>
        /// <param name="indexName">Full name of the index to drop</param>
        public static void DropIndex(string connectionString, string indexName)
        {
            if (string.IsNullOrEmpty(indexName))
                throw new ArgumentNullException(nameof(indexName));

            using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(connectionString))
            {
                conn.Open();

                string dropIndex = String.Format(DropIndexFormat, indexName);

                Npgsql.NpgsqlCommand comm = new Npgsql.NpgsqlCommand(dropIndex, conn);
                comm.ExecuteNonQuery();

            }
        }

        /// <summary>
        /// Creates a new index
        /// </summary>
        /// <param name="connectionString">DB connection string</param>
        /// <param name="chunkName">Name of the chunk table to create an index on</param>
        /// <param name="indexName">Friendly name for the index. The full index name will be constructed using the prefix 'ephemeral', the chunk name, and the provided friendly name</param>
        /// <param name="indexCriteria">Columns for the index</param>
        /// <param name="wherePredicate">Optional - allows partial index using the provided predicate</param>
        /// <returns>The full name of the created index</returns>
        public static string CreateIndex(string connectionString, string chunkName, string indexName, string indexCriteria, string wherePredicate = null)
        {
            if (string.IsNullOrEmpty(indexCriteria))
                throw new ArgumentNullException(nameof(indexCriteria));
            if (string.IsNullOrEmpty(chunkName))
                throw new ArgumentNullException(nameof(chunkName));

            // Verify we're working with a chunk. Either we accidentally called this with the full table, or timescaledb chunk naming changed...
            if (!chunkName.StartsWith("_hyper"))
                throw new ArgumentException("Chunk name does not appear to be a hypertable chunk", chunkName);

            string fullIndexName = $"ephemeral_hyper_{indexName}_{chunkName}";

            using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(connectionString))
            {
                conn.Open();

                string createIndex = String.Format(CreateEphemeralIndexFormat, fullIndexName, chunkName, indexCriteria, wherePredicate);

                Npgsql.NpgsqlCommand comm = new Npgsql.NpgsqlCommand(createIndex, conn);
                comm.ExecuteNonQuery();

            }

            return fullIndexName;
        }

        /// <summary>
        /// Create index statement
        /// {0} index name.
        /// {1} chunk table
        /// {2} index columns
        /// {3} optional - predicate for partial index
        /// </summary>
        public static readonly string CreateEphemeralIndexFormat = @"CREATE INDEX {0} ON _timescaledb_internal.{1} USING BTREE ({2}) TABLESPACE eph_idx {3};";

        /// <summary>
        /// Drops an index named {0}
        /// drop index _timescaledb_internal.{0};
        /// </summary>
        public static readonly string DropIndexFormat = @"drop index _timescaledb_internal.{0};";

        /// <summary>
        /// Retrieve indexes for table {0}
        /// select indexname from pg_indexes where tablename='{0}';
        /// </summary>
        public static readonly string GetIndexForTableFormat = @"select indexname from pg_indexes where tablename='{0}';";

        /// <summary>
        /// Retrieve the 'regular' table and all chunks associated with it
        /// </summary>
        public static readonly string GetAllChunks = @"select concat(hypertable.schema_name, '.', hypertable.table_name) as regular_table,
    concat(chunk.schema_name, '.', chunk.table_name) as chunk_table from _timescaledb_catalog.chunk
inner join _timescaledb_catalog.hypertable on hypertable.id = hypertable_id;";

        /// <summary>
        /// Retrieve the 'regular' table and all chunks associated with it.
        /// Restricted by a table check
        /// </summary>
        public static readonly string GetAllChunksLimited = @"select concat(hypertable.schema_name, '.', hypertable.table_name) as regular_table,
    concat(chunk.schema_name, '.', chunk.table_name) as chunk_table from _timescaledb_catalog.chunk
inner join _timescaledb_catalog.hypertable on hypertable.id = hypertable_id
WHERE  concat(hypertable.schema_name, '.', hypertable.table_name) in ({0});";

        /// <summary>
        /// Retrieves indexes starting with 'ephemeral_hyper'.
        /// Format for indexes we create will be prefixed with 'ephemeral', followed by the chunk name
        /// which currently means '_hyper...' will be next.
        /// select concat('_timescaledb_internal', '.', table_name), indexname from pg_indexes where indexname like 'ephemeral_hyper%';
        /// </summary>
        public static readonly string GetEphemeralIndexes = @"select concat('_timescaledb_internal', '.', tablename), indexname from pg_indexes where indexname like 'ephemeral_hyper%';";

        /// <summary>
        /// Retrieve chunk names for a hypertable. Order by chunk name which *SHOULD* give us time ordering
        /// select show_chunks::text from show_chunks('{0}') ORDERR BY show_chunks desc;
        /// </summary>
        public static readonly string ShowChunksFormats = @"select show_chunks::text from show_chunks('{0}') ORDER BY show_chunks DESC;";

        /// <summary>
        /// Retrieves all hypertables
        /// select concat(schema_name, '.', table_name) from _timescaledb_catalog.hypertables;
        /// </summary>
        public static readonly string SelectHypertables = @"select concat(schema_name, '.', table_name) from _timescaledb_catalog.hypertables;";

        /// <summary>
        /// Retrieve the newest value from a table/chunk
        /// For getting the latest timestamp,  {0} and {2} should be the same time column.
        /// {0} - column to select
        /// {1} - table to select from
        /// {2} - time column - for hypertables should be the timestamp column used
        /// select {0} from {1} order by {2} desc limit 1;
        /// </summary>
        public static readonly string SelectNewestValueFormat = "select {0} from {1} order by {2} desc limit 1;";
    }
}
