using System;
using System.Reactive.Linq;
using System.Threading.Tasks;
using KafkaConsts;

namespace RxConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var consumerGroup = args.Length > 0 ? args[0] : "RX_CONSUMER_7";
            Console.WriteLine($"Consumer group: {consumerGroup}");

            var dao = new DataAccess.Dao();
            var msgHandler = new Business.MessageHandler(dao);
            //var msgHandler = new DeduplicationDecorator(new Business.MessageHandler(dao), dao);
            var consumer = new Consumer(dao, msgHandler, consumerGroup);
            var observable = consumer.Subscribe(KafkaConfig.TopicName);

            observable
                //.Throttle(TimeSpan.FromSeconds(1))
                .Subscribe(async msg =>
            {
                await consumer.ProcessMessageAsync(msg);
                await consumer.CommitAsync();
            });
        }
    }


}