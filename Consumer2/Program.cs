using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Nito.AsyncEx;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using KafkaConsts;

namespace Consumer
{
    class Program
    {

        static void Main(string[] args)
        {
            var brokerList = "10.1.3.166:19092,10.1.3.166:29092,10.1.3.166:39092";
            var topicName = KafkaConfig.TopicName;

            var dao = new DataAccess.Dao();
            var msgHandler = new Business.MessageHandler(dao);

            //1.5 ms / msg
            var consumer = new AsyncProcessingAutoCommitConsumer(dao, msgHandler, "OLD_ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_394");



            //1.6 ms / msg
            //var consumer = new AsyncProcessingAsyncManualCommitConsumer("ASYNC_PROCESSING_ASYNC_MANUAL_COMMIT_CONSUMER_7");


            //500 ms / msg - and growing!!!!!!
            //var consumer = new AsyncProcessingSyncManualCommitConsumer("ASYNC_PROCESSING_SYNC_MANUAL_COMMIT_CONSUMER_11");


            //114 ms / msg
            //var consumer = new SyncProcessingAutoCommitConsumer("SYNC_PROCESSING_AUTO_COMMIT_CONSUMER_2");

            //114 ms / msg
            //var consumer = new SyncProcessingAsyncManualCommitConsumer("SYNC_PROCESSING_ASYNC_MANUAL_COMMIT_CONSUMER_2");

            //250 ms / msg
            //var consumer = new SyncProcessingSyncManualCommitConsumer("SYNC_PROCESSING_SYNC_MANUAL_COMMIT_CONSUMER_3");



            //consumer.ConsumeTopic(brokerList, topicName).Wait();
            consumer.ConsumeTopic(brokerList, topicName);
        }

    }
}