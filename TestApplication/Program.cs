using System;

using System.Collections.Generic;

using EphemeralIndexingService;

using System.Linq;

using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;

using Microsoft.Extensions.Logging.Console;

namespace TestApplication
{
    class Program
    {
        static void Main(string[] args)
        {

           //Demo();

            Demo(1);



            //Demo();

            //string connTest = System.IO.File.ReadAllText(@"E:\TODO\conn.txt");


            //Dictionary<string, string> map =  EphemeralIndexing.IndexHelper.ChunkToRegularLimited(connTest, new List<string>() {"stats.timeseries_data_id", "test.simple_test"});
            //foreach(string s in map.Keys)
            //{
            //    Console.WriteLine(s + "\t" + map[s]);
            //}

            ////string chunkName = "_hyper_0_1_chunk";
            ////string indexName = "test_idx";

            ////Console.WriteLine(EphemeralIndexing.IndexHelper.CreateIndex("test", chunkName, indexName, "machine_id"));

            //ConfiguredOptions co = new ConfiguredOptions();
            //co.Options.Add(new EphemeralIndexingOptions()
            //{
            //    Enabled = true,
            //    AgeToIndex = TimeSpan.FromHours(4),
            //    Hypertable = "demo.samples",
            //    TimeColumn = "sample_time",
            //    IndexCriteria = "sample_time,machine_id",
            //    Predicate = null
            //});

            //OptionsHelper.ToFile(co, @"E:\TODO\options.xml");

            //EphemeralIndexingService.ConfiguredOptions opts = OptionsHelper.FromFile(@"E:\TODO\options.xml");

            //Console.WriteLine(opts.Options.Count.ToString());



        }


        public static void Demo(int hours = 4)
        {
            // Create: TODO: Set connection string
            string connStr = System.IO.File.ReadAllText(@"E:\conn.txt");

            EphemeralIndexingService.ConfiguredOptions options = new ConfiguredOptions();
            options.ConnectionString = connStr;
            options.Options.Add(new EphemeralIndexingOptions()
            {
                AgeToIndex = TimeSpan.FromHours(hours),
                Enabled = true,
                Hypertable = "test.ephemeral_demo",
                IndexCriteria = "sample_time,metric_name",
                IndexName = "metric_time",
                Predicate = null,
                TimeColumn = "sample_time"
            });

            // Save so service can find it
            EphemeralIndexingService.OptionsHelper.ToFile(options, System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "options.xml"));

          

            // All this just to log to the console....thanks .net 5
            var configureNamedOptions = new ConfigureNamedOptions<ConsoleLoggerOptions>("", null);
            var optionsFactory = new OptionsFactory<ConsoleLoggerOptions>(new[] { configureNamedOptions }, Enumerable.Empty<IPostConfigureOptions<ConsoleLoggerOptions>>());
            var optionsMonitor = new OptionsMonitor<ConsoleLoggerOptions>(optionsFactory, Enumerable.Empty<IOptionsChangeTokenSource<ConsoleLoggerOptions>>(), new OptionsCache<ConsoleLoggerOptions>());
            var loggerFactory = new LoggerFactory(new[] { new ConsoleLoggerProvider(optionsMonitor) }, new LoggerFilterOptions { MinLevel = LogLevel.Trace });

            //ILoggerFactory loggerFactory = new LoggerFactory(
            //              new[] { new ConsoleLoggerProvider(clo) });
            ////or
            ////ILoggerFactory loggerFactory = new LoggerFactory().AddConsole();

            ILogger<EphemeralIndexingService.IndexingService> logger = loggerFactory.CreateLogger<EphemeralIndexingService.IndexingService>();
            //  logger.LogInformation("This is log message.");


            EphemeralIndexingService.IndexingService service = new IndexingService(logger);

            service.CheckDynamicIndexes();

        }
    }
}
