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
    public class AsyncProcessingAsyncManualCommitConsumer : Consumer
    {
        public AsyncProcessingAsyncManualCommitConsumer(Dao dao, IMessageHandler handler, string consumerGroup) 
            : base(dao, handler, consumerGroup)
        {
        }

        protected override bool EnableAutoCommit => false;

        protected override async Task ExecuteProcessMessage(Func<Task> processMessage)
        {
            await processMessage();
        }

        protected override async Task ExecuteCommitOffset(Func<Task> commitOffset)
        {
            await commitOffset();
        }
        
    }
}
