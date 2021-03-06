﻿using System;
using System.Threading.Tasks;
using Business;
using KafkaConsts;

namespace AnotherChunkedPollAsyncProcessingAutoCommitConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            string consumerGroup = args.Length > 0 ? args[0] : "NEW__ANOTHER_CHUNKED_POLL_ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_28";
            Console.WriteLine($"Consumer group: {consumerGroup}");

            try
            {
                //https://stackoverflow.com/questions/17630506/async-at-console-app-in-c
                //MainAsync(consumerGroup).Wait();
                MainAsync(consumerGroup).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occured: {ex}");
            }

            //AsyncContext.Run(() => MainAsync(args));

        }

        static async Task MainAsync(string consumerGroup)
        {
            var dao = new DataAccess.Dao();
            //var msgHandler = new MessageHandler(dao);
            //var msgHandler = new DeduplicationDecorator(new Business.MessageHandler(dao), dao);
            var msgHandler = new SlowExecutionDecorator(new Business.MessageHandler(dao));
            var consumer = new Consumer(dao, msgHandler, consumerGroup);
            consumer.Subscribe(KafkaConfig.TopicName);

            var chunkSize = 100;

            while (true)
            {
                if (consumer.InProcessingMessages < chunkSize)
                {
                    for (var i = 0; i < chunkSize; i++) {
                        consumer.Poll();
                    }
                }
                else
                    await Task.Delay(1);

            }
        }
    }
}