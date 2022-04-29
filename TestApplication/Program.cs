using System;


using EphemeralIndexingService;

namespace TestApplication
{
    class Program
    {
        static void Main(string[] args)
        {
            //string chunkName = "_hyper_0_1_chunk";
            //string indexName = "test_idx";

            //Console.WriteLine(EphemeralIndexing.IndexHelper.CreateIndex("test", chunkName, indexName, "machine_id"));

            ConfiguredOptions co = new ConfiguredOptions();
            co.Options.Add(new EphemeralIndexingOptions()
            {
                Enabled = true,
                AgeToIndex = TimeSpan.FromHours(4),
                Hypertable = "demo.samples",
                TimeColumn = "sample_time",
                IndexCriteria = "sample_time,machine_id",
                Predicate = null
            });

            OptionsHelper.ToFile(co, @"E:\TODO\options.xml");

            EphemeralIndexingService.ConfiguredOptions opts = OptionsHelper.FromFile(@"E:\TODO\options.xml");

            Console.WriteLine(opts.Options.Count.ToString());
           


        }
    }
}
