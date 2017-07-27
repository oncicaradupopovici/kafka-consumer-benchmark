using System;
using System.Threading.Tasks;
using Business;
using KafkaConsts;
using Nito.AsyncEx;

namespace SerialConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var consumerGroup = args.Length > 0 ? args[0] : "SERIAL_CONSUMER_3";
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
            //AsyncContext.Run(() => MainAsync(consumerGroup));
        }

        static Task MainAsync(string consumerGroup)
        {
            var dao = new DataAccess.Dao();
            var msgHandler = new Business.MessageHandler(dao);
            //var msgHandler = new DeduplicationDecorator(new Business.MessageHandler(dao), dao);
            var consumer = new Consumer(dao, msgHandler, consumerGroup);
            return consumer.SubscribeAsync(KafkaConfig.TopicName);
        }
    }
}