using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Consumer
{
    public class AsyncProcessingAsyncManualCommitConsumer : Consumer
    {
        public AsyncProcessingAsyncManualCommitConsumer(string consumerGroup) 
            : base(consumerGroup)
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
