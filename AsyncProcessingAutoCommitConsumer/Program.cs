using System;
using System.Threading;
using System.Threading.Tasks;
using Business;
using KafkaConsts;

namespace AsyncProcessingAutoCommitConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var consumerGroup = args.Length > 0 ? args[0] : "NEW__ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_79";
            Console.WriteLine($"Consumer group: {consumerGroup}");

            var dao = new DataAccess.Dao();
            //var msgHandler = new MessageHandler(dao);
            //var msgHandler = new Business.BreakAt1000MsgDecorator(new Business.MessageHandler(dao));
            var msgHandler = new DeduplicationDecorator(new Business.MessageHandler(dao), dao);
            var consumer = new Consumer(dao, msgHandler, consumerGroup);
            consumer.Subscribe(KafkaConfig.TopicName);


            while (true)
            {
                consumer.Poll();
            }
        }

    }
}