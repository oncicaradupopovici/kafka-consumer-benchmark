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
    public class AsyncProcessingSyncManualCommitConsumer : Consumer
    {
        public AsyncProcessingSyncManualCommitConsumer(Dao dao, IMessageHandler handler, string consumerGroup) 
            : base(dao, handler, consumerGroup)
        {
        }

        protected override bool EnableAutoCommit => false;

        protected override async Task ExecuteProcessMessage(Func<Task> processMessage)
        {
            await processMessage(); //async
        }

        protected override Task ExecuteCommitOffset(Func<Task> commitOffset)
        {
            commitOffset().Wait(); //block
            return Task.FromResult("done");
        }
        
    }
}
