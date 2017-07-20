using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Business;
using Business.Contracts;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using DataAccess;

namespace Consumer
{
    public class SyncProcessingAutoCommitConsumer : Consumer
    {
        public SyncProcessingAutoCommitConsumer(Dao dao, IMessageHandler handler, string consumerGroup) 
            : base(dao, handler, consumerGroup)
        {
        }

        protected override bool EnableAutoCommit => true;

        protected override Task ExecuteProcessMessage(Func<Task> processMessage)
        {
            processMessage().Wait(); //block
            return Task.FromResult("done");
        }

        protected override Task ExecuteCommitOffset(Func<Task> commitOffset)
        {
            return Task.FromResult("nothing to do here");
        }

    }
}
