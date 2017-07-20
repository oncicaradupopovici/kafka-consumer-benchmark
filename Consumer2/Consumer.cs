using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System.Data.SqlClient;
using System.Threading;
using DataAccess;
using Business;
using Business.Contracts;

namespace Consumer
{
    public abstract class Consumer
    {
        private readonly Dao _dao;
        private readonly IMessageHandler _messageHandler;
        private readonly string _consumerGroup;

        protected abstract bool EnableAutoCommit { get; }

        protected Consumer(DataAccess.Dao dao, IMessageHandler handler, string consumerGroup)
        {
            this._dao = dao;
            this._messageHandler = handler;
            this._consumerGroup = consumerGroup;
        }

        public void ConsumeTopic(string brokerList, string topicName)
        {

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
                    var businessMsg = new Business.Message(msg.Value, msg.Key, _consumerGroup, consumer.MemberId, topicName, msg.Partition, msg.Offset.Value);
                    await _messageHandler.HandleAsync(businessMsg);

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


        private async Task CommitMessageAsync(Consumer<string, string> consumer)
        {
            await consumer.CommitAsync();
        }


        
    }
}
