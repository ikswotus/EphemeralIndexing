using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace EphemeralIndexingService
{
    public class IndexingService : BackgroundService
    {
        private readonly ILogger<IndexingService> _logger;

        private readonly string _optionsPath;

        public IndexingService(ILogger<IndexingService> logger)
        {
            _logger = logger;
            _optionsPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "options.xml");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

                CheckDynamicIndexes();

                await Task.Delay(1000 * 3, stoppingToken);
            }
        }

        private DateTime _lastChunkCheck = DateTime.MinValue;

        private Dictionary<string, string> _chunkMap = null;

        private List<EphemeralIndexing.ActiveIndex> _indices = new List<EphemeralIndexing.ActiveIndex>();
        private Dictionary<string, EphemeralIndexing.ActiveIndex> _allIndices = new Dictionary<string, EphemeralIndexing.ActiveIndex>();


        private ConfiguredOptions _options = null;
        private DateTime _lastOptionsLoad = DateTime.MinValue;

        /// <summary>
        /// Look for old indices and sets up new ones on the configured options
        /// 
        /// TODO: Drop ephemeral indexes that arent in our criteria? Orphaned/Abandoned...
        /// </summary>
        /// <param name="connectionString"></param>
        public void CheckDynamicIndexes()
        {
            try
            {
                if(!File.Exists(_optionsPath))
                {
                    _logger.LogError("No options file found");
                    return;
                }
                // New options?
                DateTime lwt = File.GetLastWriteTimeUtc(_optionsPath);
                if(_options == null || (lwt - _lastOptionsLoad).TotalSeconds > 1)
                {
                    // Reload options
                    try
                    {
                        _options = OptionsHelper.FromFile(_optionsPath);
                        _lastOptionsLoad = lwt;
                    }
                    catch(Exception exc)
                    {
                        _logger.LogError("Failed to load options: " + exc);
                        if (_options == null)
                            return; // else use old options for now
                    }
                }
                string connectionString = _options.ConnectionString;
                if(String.IsNullOrEmpty(connectionString))
                {
                    _logger.LogError("No connection string");
                    return;
                }

                // TODO: need to get config file from somewhere

             

                if(_options.Options == null)
                {
                    _logger.LogError("No configured indexes");
                    return;
                }

                bool updatedChunkList = false;
                // Get the chunks for all tables from the database
                // 
                // TODO: 1 ) ChunkToRegular should only map chunks for active hypertables we're going to index...we dont need everything here
                //
                //
                // TODO: 2 ) Re-checking on hour change only works for hourly chunks, may need to check every few minutes? This shouldn't be
                // too terribly expensive. The most expensive check will be rows/dates from actual chunk tables
                // but we could diff any new/removed indices and only perform that on a change.
                if(_lastChunkCheck == DateTime.MinValue || _lastChunkCheck.Hour != DateTime.UtcNow.Hour)
                {
                    // Get all current indexes
                    _allIndices.Clear();

                    List<Tuple<string, string>> currentIndexes = EphemeralIndexing.IndexHelper.GetAllEphemeralIndexes(connectionString);
                    foreach (Tuple<string, string> tup in currentIndexes)
                    {
                        EphemeralIndexing.ActiveIndex i = new EphemeralIndexing.ActiveIndex();
                        i.ChunkName = tup.Item1;
                        i.IndexName = tup.Item2;
                        _allIndices.Add(tup.Item1, i);
                    }

                    if (_options.Options.Any(r => r.Enabled))
                    {
                        _logger.LogDebug("Getting latest chunk map");

                        IEnumerable<String> activeHypertables = _options.Options.Where(r => r.Enabled).Select(s => s.Hypertable);

                        _chunkMap = EphemeralIndexing.IndexHelper.ChunkToRegularLimited(connectionString, activeHypertables);

                        // Populate active
                        foreach(string t in _chunkMap.Keys)
                        {
                            if (_allIndices.ContainsKey(t))
                                _allIndices[t].TableName = _chunkMap[t];
                        }

                     
                    }
                    updatedChunkList = true;
                    _lastChunkCheck = DateTime.UtcNow;
                }

                if(!updatedChunkList)
                {
                    _logger.LogTrace("No new chunks detected - skipping index checks");
                    return;
                }

                // Cleanup any removed indices
                foreach(string chunk in _allIndices.Keys)
                {
                    if (String.IsNullOrEmpty(_allIndices[chunk].TableName))
                    {
                        _logger.LogDebug("Removing index " + _allIndices[chunk].IndexName + " from chunk " + chunk + " because it is no longer an active hypertable");

                        EphemeralIndexing.IndexHelper.DropIndex(connectionString, _allIndices[chunk].IndexName);
                        
                        // Rather than modify the collection, set this as a flag to indicate its been removed.
                        _allIndices[chunk].IndexName = null;
                    }

                }


                HashSet<string> hadIndex = new HashSet<string>();

                // next cleanup old indexes
                // Match by both hypertable and friendly index name to allow multiple indexes on the same hypertable
                // Track existing in hadIndex so we dont have to check later
                foreach (EphemeralIndexingService.EphemeralIndexingOptions activeOpts in _options.Options.Where(r => r.Enabled))
                {
                    // Get indices, skipping any removed
                    IEnumerable<EphemeralIndexing.ActiveIndex> matching =
                        _allIndices.Values.Where(t => 
                            String.Equals(t.TableName, activeOpts.Hypertable, StringComparison.OrdinalIgnoreCase) &&
                            !String.IsNullOrEmpty(t.IndexName) &&
                            t.IndexName.IndexOf(activeOpts.IndexName, StringComparison.OrdinalIgnoreCase) != -1);

                  
                    foreach (EphemeralIndexing.ActiveIndex index in matching.ToArray())
                    {
                        DateTime newest = EphemeralIndexing.IndexHelper.GetNewestDate(connectionString, index.ChunkName, activeOpts.TimeColumn);
                        if (newest.Add(activeOpts.AgeToIndex) < DateTime.UtcNow)
                        {
                            _logger.LogInformation("Chunk " + index.ChunkName + " age exceeds indexing window, dropping: " + index.IndexName);

                            EphemeralIndexing.IndexHelper.DropIndex(connectionString, index.IndexName);

                            _indices.Remove(index);
                        }
                        else
                        {
                            _logger.LogTrace("Chunk " + index.ChunkName + " already indexed by: " + index.IndexName);
                            hadIndex.Add(index.ChunkName);
                        }
                    }
                }


                // Walk any active index, and see if we've indexed all the recent chunks
                foreach (EphemeralIndexingOptions activeOpts in _options.Options.Where(e => e.Enabled))
                {
                    try
                    {
                        if(string.IsNullOrEmpty(activeOpts.Hypertable) || string.IsNullOrEmpty(activeOpts.IndexName) || string.IsNullOrEmpty(activeOpts.IndexCriteria))
                        {
                            _logger.LogError("Skipping invalid options");
                            continue;
                        }

                        _logger.LogInformation("Checking indexing for: " + activeOpts.Hypertable);

                     

                        // Setup new index
                        List<string> chunks = EphemeralIndexing.IndexHelper.GetChunks(connectionString, activeOpts.Hypertable);
                        // Chunks should now be ordered
                        for(int i = 0; i < chunks.Count; i++)
                        {
                            if (hadIndex.Contains(chunks[i]))
                                continue;// already indexed + still valid
                            DateTime newest = EphemeralIndexing.IndexHelper.GetNewestDate(connectionString, chunks[i], activeOpts.TimeColumn);
                            if (newest.Add(activeOpts.AgeToIndex) < DateTime.UtcNow)
                                break;// All chunks are indexed
                           
                            string c = chunks[i].Substring(chunks[i].IndexOf('.') + 1);
                            string idx = EphemeralIndexing.IndexHelper.CreateIndex(connectionString, c, activeOpts.IndexName, activeOpts.IndexCriteria, activeOpts.Predicate);
                            _logger.LogInformation("Created index " + idx + " for chunk: " + chunks[i]);
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
