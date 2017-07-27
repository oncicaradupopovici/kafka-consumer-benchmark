using System;
using System.Collections.Generic;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;
using Business;
using Business.Contracts;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using DataAccess;

namespace RxConsumer
{
    public class Consumer
    {
        private Consumer<string, string> _consumer;
        private string _consumerGroup;
        private Dao _dao;
        private IMessageHandler _handler;

        public Consumer(DataAccess.Dao dao, IMessageHandler handler, string consumerGroup)
        {
            _dao = dao;
            _handler = handler;
            _consumerGroup = consumerGroup;

            var config = new Dictionary<string, object>
            {
                {"group.id", _consumerGroup},
                {"bootstrap.servers", "10.1.3.166:19092,10.1.3.166:29092,10.1.3.166:39092"},
                {"enable.auto.commit", "false"},
                {"auto.offset.reset", "earliest"}
            };
            _consumer = new Consumer<string, string>(config, new StringDeserializer(Encoding.UTF8),
                new StringDeserializer(Encoding.UTF8));
        }

        public IObservable<Message<string, string>> Subscribe(string topicName)
        {

            //_consumer.OnMessage += (_, msg) =>
            //{
            //    //_handler.HandleAsync(msg, _consumer.MemberId, _consumerGroup).Wait();
            //    //CommitMessageAsync(_consumer).Wait();

            //};

            _consumer.OnPartitionsAssigned += (_, partitions) =>
            {
                Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]");
                _consumer.Assign(partitions);

            };

            _consumer.OnPartitionsRevoked += (_, partitions) =>
            {
                Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
                _consumer.Unassign();
            };

            _consumer.Subscribe(topicName);

            //var observable = Observable.FromEventPattern<Message<string, string>>(
            //    handler => _consumer.OnMessage += handler,
            //    handler => _consumer.OnMessage -= handler
            //);



            return Observable.Create<Message<string, string>>(observer =>
            {
                var loop = NewThreadScheduler.Default.ScheduleLongRunning(cancel =>
                {
                    while (!cancel.IsDisposed)
                    {
                        if (_consumer.Consume(out Message<string, string> msg, TimeSpan.FromMilliseconds(100)))
                            observer.OnNext(msg);
                    }
                });

                return loop;
            });


        }



        public async Task ProcessMessageAsync(Message<string, string> msg)
        {
            var businessMsg = new Business.Message(msg.Value, msg.Key, _consumerGroup, _consumer.MemberId, msg.Topic, msg.Partition, msg.Offset.Value);
            await _handler.HandleAsync(businessMsg);
        }

        public async Task CommitAsync()
        {
            await _consumer.CommitAsync();
        }
    }
}
