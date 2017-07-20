using System;
using System.Threading.Tasks;
using Business;
using KafkaConsts;
using Nito.AsyncEx;

namespace NonBlockingIoSyncProcessingSyncCommitConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var consumerGroup = args.Length > 0 ? args[0] : "NEW__NON_BLOCKING_IO_SYNC_PROCESSING_SYNC_COMMIT_CONSUMER_17";
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
            //var msgHandler = new Business.MessageHandler(dao);
            var msgHandler = new DeduplicationDecorator(new Business.MessageHandler(dao), dao);
            var consumer = new Consumer(dao, msgHandler, consumerGroup);
            consumer.Subscribe(KafkaConfig.TopicName);

            while (true)
            {
                await consumer.ConsumeProcessAndCommitAsync();
            }
        }
    }
}