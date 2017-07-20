using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Consumer
{
    public class SyncProcessingAsyncManualCommitConsumer : Consumer
    {
        public SyncProcessingAsyncManualCommitConsumer(string consumerGroup) 
            : base(consumerGroup)
        {
        }

        protected override bool EnableAutoCommit => false;

        protected override Task ExecuteProcessMessage(Func<Task> processMessage)
        {
            processMessage().Wait(); //block
            return Task.FromResult("done");
        }

        protected override async Task ExecuteCommitOffset(Func<Task> commitOffset)
        {
            await commitOffset(); //async
        }
        
    }
}
