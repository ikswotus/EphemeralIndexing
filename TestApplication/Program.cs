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

            string connString = args[0]; // System.IO.File.ReadAllText(@"E:\conn.txt");


            ILogger<EphemeralIndexingService.IndexingService> logger = GetLogger();

            // Sets up initial indexing on some chunks
            Demo(connString, logger);

            logger.LogInformation("Initial chunk indexing complete - Press key continue demo");
            Console.ReadKey();

            Demo(connString, logger);

            logger.LogInformation("Index check complete - Press key to continue demo and reduce indexing by adjusting retention");
            Console.ReadKey();

            Demo(connString, logger, 1);

            logger.LogInformation("Retenion adjustment complete - Press key to continue demo and remove indexing on test hypertable");
            Console.ReadKey();
            
            Demo(connString, logger, 8, true);


            logger.LogInformation("Demo Complete!");
        }

        /// <summary>
        /// Logging is a bit of a nightmare...get a console logger for use in the demo
        /// </summary>
        /// <returns></returns>
        public static ILogger<EphemeralIndexingService.IndexingService> GetLogger()
        {
           
            var configureNamedOptions = new ConfigureNamedOptions<ConsoleLoggerOptions>("", null);
            var optionsFactory = new OptionsFactory<ConsoleLoggerOptions>(new[] { configureNamedOptions }, Enumerable.Empty<IPostConfigureOptions<ConsoleLoggerOptions>>());
            var optionsMonitor = new OptionsMonitor<ConsoleLoggerOptions>(optionsFactory, Enumerable.Empty<IOptionsChangeTokenSource<ConsoleLoggerOptions>>(), new OptionsCache<ConsoleLoggerOptions>());
            var loggerFactory = new LoggerFactory(new[] { new ConsoleLoggerProvider(optionsMonitor) }, new LoggerFilterOptions { MinLevel = LogLevel.Trace });

            ILogger<EphemeralIndexingService.IndexingService> logger = loggerFactory.CreateLogger<EphemeralIndexingService.IndexingService>();
            
            return logger;
        }

        public static void Demo(string connectionString, ILogger<EphemeralIndexingService.IndexingService> logger, int hours = 4, bool clear = false)
        {
            
            EphemeralIndexingService.ConfiguredOptions options = new ConfiguredOptions();
            options.ConnectionString = connectionString;

            if (!clear)
            {
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
            }
            // Save so service can find it
            EphemeralIndexingService.OptionsHelper.ToFile(options, System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "options.xml"));

            EphemeralIndexingService.IndexingService service = new IndexingService(logger);

            service.CheckDynamicIndexes();

        }
    }
}
