using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Nito.AsyncEx;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Consumer
{
    class Program
    {

        static void Main(string[] args)
        {
            try
            {
                AsyncContext.Run(() => MainAsync(args));
                //MainAsync(args).Wait();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex);
            }
        }

        static async Task MainAsync(string[] args)
        {
            var brokerList = "10.1.3.166:19092,10.1.3.166:29092,10.1.3.166:39092";
            var topicName = "test-topic-2";

            //With no synhronization context 1.5 ms / msg
            var consumer = new AsyncProcessingAutoCommitConsumer("SINGLE_THREADED_NEW_ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_25");



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
            await consumer.SubscribeToTopicAsync(brokerList, topicName);
            while (true)
            {
                await consumer.ConsumeNewMessageAsync();
            }
        }
    }
}