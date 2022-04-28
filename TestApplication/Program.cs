using System;

namespace TestApplication
{
    class Program
    {
        static void Main(string[] args)
        {
            string chunkName = "_hyper_0_1_chunk";
            string indexName = "test_idx";

            Console.WriteLine(EphemeralIndexing.IndexHelper.CreateIndex("test", chunkName, indexName, "machine_id"));

        }
    }
}
