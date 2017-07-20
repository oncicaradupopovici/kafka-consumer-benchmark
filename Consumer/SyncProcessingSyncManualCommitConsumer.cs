using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Consumer
{
    public class SyncProcessingSyncManualCommitConsumer : Consumer
    {
        public SyncProcessingSyncManualCommitConsumer(string consumerGroup) 
            : base(consumerGroup)
        {
        }

        protected override bool EnableAutoCommit => false;

        protected override Task ExecuteProcessMessage(Func<Task> processMessage)
        {
            processMessage().Wait(); //block
            return Task.FromResult("done");
        }

        protected override Task ExecuteCommitOffset(Func<Task> commitOffset)
        {
            commitOffset().Wait(); //block
            return Task.FromResult("done");
        }
        
    }
}
