using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EphemeralIndexingService
{
    public class IndexingService : BackgroundService
    {
        private readonly ILogger<IndexingService> _logger;

        public IndexingService(ILogger<IndexingService> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                await Task.Delay(1000, stoppingToken);
            }
        }

        private DateTime _lastChunkCheck = DateTime.MinValue;

        private Dictionary<string, string> _chunkMap = null;

        private List<EphemeralIndexing.ActiveIndex> _indices = new List<EphemeralIndexing.ActiveIndex>();

        /// <summary>
        /// Look for old indices and sets up new ones on the configured options
        /// 
        /// TODO: Drop ephemeral indexes that arent in our criteria? Orphaned/Abandoned...
        /// </summary>
        /// <param name="connectionString"></param>
        public void CheckDynamicIndexes(string connectionString)
        {
            try
            {
                // TODO: need to get config file from somewhere

                List<EphemeralIndexingOptions> opts = new List<EphemeralIndexingOptions>();

                if(opts == null || opts.Count == 0)
                {
                    return;// nothing to do unless we need to prune any existing...
                }

                // TODO: Works if we're chunked on hours...might want to just run every x minutes
                if(_lastChunkCheck == DateTime.MinValue || _lastChunkCheck.Hour != DateTime.UtcNow.Hour)
                {
                    _logger.LogDebug("Getting latest chunk map");
                    _chunkMap = EphemeralIndexing.IndexHelper.ChunkToRegular(connectionString);
                    _indices.Clear();
                    List<Tuple<string, string>> currentIndexes = EphemeralIndexing.IndexHelper.GetAllEphemeralIndexes(connectionString);
                    foreach(Tuple<string, string> tup in currentIndexes)
                    {
                        EphemeralIndexing.ActiveIndex i = new EphemeralIndexing.ActiveIndex();
                        i.ChunkName = tup.Item1;
                        i.IndexName = tup.Item2;
                        if (_chunkMap.ContainsKey(tup.Item1))
                        {
                            i.TableName = _chunkMap[tup.Item2];
                        }
                        else
                        {
                            _logger.LogError("Failed to locate table for chunk: " + tup.Item1);
                            continue;// skip it
                        }
                        _indices.Add(i);
                    }

                    _lastChunkCheck = DateTime.UtcNow;
                }

                foreach(EphemeralIndexingOptions activeOpts in opts.Where(e => e.Enabled))
                {
                    try
                    {
                        if(string.IsNullOrEmpty(activeOpts.Hypertable) || string.IsNullOrEmpty(activeOpts.IndexName) || string.IsNullOrEmpty(activeOpts.IndexCriteria))
                        {
                            _logger.LogError("Skipping invalid options");
                            continue;
                        }

                        _logger.LogInformation("Checking indexing for: " + activeOpts.Hypertable);

                        // First, cleanup any old indices
                        // Match by both hypertable and friendly index name to allow multiple indexes on the same hypertable
                        IEnumerable<EphemeralIndexing.ActiveIndex> matching = _indices.Where(t => String.Equals(t.TableName, activeOpts.Hypertable, StringComparison.OrdinalIgnoreCase) &&
                            t.IndexName.EndsWith(activeOpts.IndexName, StringComparison.OrdinalIgnoreCase));
                        HashSet<string> hadIndex = new HashSet<string>();
                        foreach(EphemeralIndexing.ActiveIndex index in matching.ToArray())
                        {
                            DateTime newest = EphemeralIndexing.IndexHelper.GetNewestDate(connectionString, index.ChunkName, activeOpts.TimeColumn);
                            if(newest.Add(activeOpts.AgeToIndex) < DateTime.UtcNow)
                            {
                                _logger.LogInformation("Chunk " + index.ChunkName + " age exceeds indexing window, dropping: " + index.IndexName);

                                EphemeralIndexing.IndexHelper.DropIndex(connectionString, index.IndexName);

                                _indices.Remove(index);
                            }
                            else
                            {
                                hadIndex.Add(index.ChunkName);
                            }
                        }
                        // Setup new index
                        List<string> chunks = EphemeralIndexing.IndexHelper.GetChunks(connectionString, activeOpts.Hypertable);
                        // Walk in reverse since newest chunks are at end and we can stop early
                        for(int i = chunks.Count - 1; i >= 0; i--)
                        {
                            if (hadIndex.Contains(chunks[i]))
                                continue;// already indexed + still valid
                            DateTime newest = EphemeralIndexing.IndexHelper.GetNewestDate(connectionString, chunks[i], activeOpts.TimeColumn);
                            if (newest.Add(activeOpts.AgeToIndex) < DateTime.UtcNow)
                                break;// All chunks are indexed
                            _logger.LogInformation("Creating new index for chunk: " + chunks[i]);
                            string c = chunks[i].Substring(chunks[i].IndexOf('.') + 1);
                            string idx = EphemeralIndexing.IndexHelper.CreateIndex(connectionString, c, activeOpts.IndexName, activeOpts.IndexCriteria, activeOpts.Predicate);
                            _indices.Add(new EphemeralIndexing.ActiveIndex() { ChunkName = chunks[i], TableName = activeOpts.Hypertable, IndexName = idx });
                        }
                    }
                    catch(Exception exc)
                    {
                        _logger.LogError("Failure in options for table {0} with friendly {1}. Exc: {2}", activeOpts.Hypertable, activeOpts.IndexName, exc);
                    }
                }


            }
            catch(Exception exc)
            {
                _logger.LogError("Error in indexing loop: {0}", exc);
            }
        }

    }
}
