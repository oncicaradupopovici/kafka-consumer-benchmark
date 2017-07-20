using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System.Data.SqlClient;
using System.Threading;

namespace Consumer
{
    public abstract class Consumer
    {
        private readonly Dao _dao;
        private readonly string _consumerGroup;

        protected abstract bool EnableAutoCommit { get; }

        protected Consumer(string consumerGroup)
        {
            this._dao = new Dao();
            this._consumerGroup = consumerGroup;
        }

        public async Task ConsumeTopic(string brokerList, string topicName)
        {

            await _dao.EnsureTableCreated(_consumerGroup);

            var config = new Dictionary<string, object>
            {
                {"group.id", _consumerGroup},
                {"bootstrap.servers", brokerList},
                {"enable.auto.commit", EnableAutoCommit? "true" : "false"},
                {"auto.offset.reset", "earliest"}
            };

            using (var consumer = new Consumer<string, string>(config, new StringDeserializer(Encoding.UTF8),
            new StringDeserializer(Encoding.UTF8)))
            {
                consumer.OnMessage += async (_, msg) =>
                {
                    await ExecuteProcessMessage(async () =>
                    {
                        await ProcessMessageAsync(msg, consumer);
                    });

                    if (!EnableAutoCommit)
                    {
                        await ExecuteCommitOffset(async () =>
                        {
                            await CommitMessageAsync(consumer);
                        });
                    };
                   
                };

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]");
                    consumer.Assign(partitions);
                };

                consumer.OnPartitionsRevoked += (_, partitions) =>
                {
                    Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
                    consumer.Unassign();
                };

                consumer.Subscribe(topicName);

                var cancelled = false;
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                while (!cancelled)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }
            }

        }


        protected abstract Task ExecuteProcessMessage(Func<Task> processMessage);
        protected abstract Task ExecuteCommitOffset(Func<Task> commitOffset);


        private async Task ProcessMessageAsync(Message<string, string> msg, Consumer<string, string> consumer)
        {
            Console.WriteLine($"Message received {msg.Value}");

            await _dao.SimulateADatabaseDelayAsync(10); //simulate some async wait - like a database read
            Thread.Sleep(10); //simulate some synchronuos work
            await _dao.PersistMessageAsync(msg, consumer, _consumerGroup);
        }
        private async Task CommitMessageAsync(Consumer<string, string> consumer)
        {
            await consumer.CommitAsync();
        }


        
    }
}
