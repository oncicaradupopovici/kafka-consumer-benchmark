using System;
using System.Threading.Tasks;
using Business;
using KafkaConsts;

namespace ChunkedPollAsyncProcessingBatchSyncCommitConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var consumerGroup = args.Length > 0 ? args[0] : "NEW__CHUNKED_POLL_ASYNC_PROCESSING_BATCH_SYNC_COMMIT_CONSUMER_11";
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

            var chunkSize = 100;
            var isFirstTime = true;

            while (true)
            {
                if (consumer.InProcessingMessages == 0)
                {
                    if(!isFirstTime)
                        await consumer.CommitAsync();

                    for (var i = 0; i < chunkSize; i++)
                    {
                        consumer.Poll();
                    }

                    isFirstTime = false;
                }
                else
                {
                    await Task.Delay(1);
                }

            }



        }
    }
}