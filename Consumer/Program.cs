using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var brokerList = "10.1.3.166:19092,10.1.3.166:29092,10.1.3.166:39092";
            var topicName = "test-topic-2";

            var consumer = new AsyncProcessingAutoCommitConsumer("ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_NETFRAMEWORK_2");
            //var consumer = new AsyncProcessingAsyncManualCommitConsumer("ASYNC_PROCESSING_ASYNC_MANUAL_COMMIT_CONSUMER_6");
            //var consumer = new AsyncProcessingSyncManualCommitConsumer("ASYNC_PROCESSING_SYNC_MANUAL_COMMIT_CONSUMER_9");

            //var consumer = new SyncProcessingSyncManualCommitConsumer("SYNC_PROCESSING_SYNC_MANUAL_COMMIT_CONSUMER_2");
            //var consumer = new SyncProcessingAsyncManualCommitConsumer("SYNC_PROCESSING_ASYNC_MANUAL_COMMIT_CONSUMER_1");
            //var consumer = new SyncProcessingAutoCommitConsumer("SYNC_PROCESSING_AUTO_COMMIT_CONSUMER_1");

            consumer.ConsumeTopic(brokerList,topicName).Wait();
        }
    }
}
