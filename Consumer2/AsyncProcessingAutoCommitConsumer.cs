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
    public class AsyncProcessingAutoCommitConsumer : Consumer
    {
        public AsyncProcessingAutoCommitConsumer(Dao dao, IMessageHandler handler, string consumerGroup) 
            : base(dao, handler, consumerGroup)
        {
        }

        protected override bool EnableAutoCommit => true;

        protected override async Task ExecuteProcessMessage(Func<Task> processMessage)
        {
            await processMessage(); //async
        }

        protected override Task ExecuteCommitOffset(Func<Task> commitOffset)
        {
            return Task.FromResult("nothing to do here");
        }


        
    }
}
