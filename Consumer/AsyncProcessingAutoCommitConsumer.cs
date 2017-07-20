using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Consumer
{
    public class AsyncProcessingAutoCommitConsumer : Consumer
    {
        public AsyncProcessingAutoCommitConsumer(string consumerGroup) 
            : base(consumerGroup)
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
