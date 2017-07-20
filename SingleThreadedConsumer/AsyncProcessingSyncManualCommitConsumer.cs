using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Consumer
{
    public class AsyncProcessingSyncManualCommitConsumer : Consumer
    {
        public AsyncProcessingSyncManualCommitConsumer(string consumerGroup) 
            : base(consumerGroup)
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
