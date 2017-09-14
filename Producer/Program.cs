using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new Dictionary<string, object> {
                { "bootstrap.servers", "10.1.3.220:30087" },
                //{ "socket.timeout.ms", 6000 },
                //{"socket.blocking.max.ms", 1000 },
                //{"session.timeout.ms", 6000 },
                //{"metadata.request.timeout.ms", 6000 },
            };
            var topicName = "test-topic-7";
            var msgCnt = 100;
            using (var producer = new Producer<string, string>(config, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                var tasks = new Task[msgCnt];
                for (var i = 0; i < msgCnt; i++)
                {
                    //var key = i % 10;
                    //var deliveryReport = producer.ProduceAsync(topicName, null, i.ToString());
                    //var result = deliveryReport.Result; // synchronously waits for message to be produced.
                    //Console.WriteLine($"Partition: {result.Partition}, Offset: {result.Offset} Error: {result.Error}");
                    tasks[i] = producer.ProduceAsync(topicName, null, i.ToString()).ContinueWith(t => Console.WriteLine(t.Result.Value));
                }

                Task.WaitAll(tasks); //block
                Console.WriteLine("Done");
                Console.ReadKey();

            }
        }
    }
}
