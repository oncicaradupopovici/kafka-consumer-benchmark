using System;
using System.Threading.Tasks;
using Business;
using KafkaConsts;
using Nito.AsyncEx;

namespace ChunkedPollAsyncProcessingAutoCommitConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var consumerGroup = args.Length > 0 ? args[0] : "NEW__CHUNKED_POLL_ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_14";
            Console.WriteLine($"Consumer group: {consumerGroup}");

            try
            {
                //https://stackoverflow.com/questions/17630506/async-at-console-app-in-c
                //MainAsync(consumerGroup).Wait();
                MainAsync(consumerGroup).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occured: {ex}");
            }
            //AsyncContext.Run(() => MainAsync(args));
        }

        static async Task MainAsync(string consumerGroup)
        {
            var dao = new DataAccess.Dao();
            var msgHandler = new Business.MessageHandler(dao);
            //var msgHandler = new DeduplicationDecorator(new Business.MessageHandler(dao), dao);
            var consumer = new Consumer(dao, msgHandler, consumerGroup);
            consumer.Subscribe(KafkaConfig.TopicName);

            var chunkSize = 10;
            var chunkIndex = 0;

            while (true)
            {
                consumer.Poll();
                if (chunkIndex++ == chunkSize)
                {
                    chunkIndex = 0;
                    await Task.Delay(1);
                }
            }
        }
    }
}