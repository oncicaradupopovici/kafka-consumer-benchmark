using System;
using System.Threading.Tasks;
using Business;
using KafkaConsts;

namespace SyncProcessingSyncCommitConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var consumerGroup = args.Length > 0 ? args[0] : "NEW__SYNC_PROCESSING_SYNC_COMMIT_CONSUMER_12";
            Console.WriteLine($"Consumer group: {consumerGroup}");

            var dao = new DataAccess.Dao();
            var msgHandler = new Business.MessageHandler(dao);
            //var msgHandler = new DeduplicationDecorator(new Business.MessageHandler(dao), dao);
            var consumer = new Consumer(dao, msgHandler, consumerGroup);
            consumer.Subscribe(KafkaConfig.TopicName);

            while (true)
            {
                consumer.Poll();
            }
        }
    }
}