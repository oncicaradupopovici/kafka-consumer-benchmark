﻿using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Orchestrator
{
    class Program
    {
        static void Main(string[] args)
        {
            //TestScalability("AsyncProcessingAutoCommitConsumer", "SCALABILITY_TEST__ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_2");
            //TestScalability("ChunkedPollAsyncProcessingAutoCommitConsumer", "SCALABILITY_TEST__CHUNKED_POLL_ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_1");
            //TestScalability("AnotherChunkedPollAsyncProcessingAutoCommitConsumer", "SCALABILITY_TEST__ANOTHER_CHUNKED_POLL_ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_1");
            //TestScalability("SyncProcessingSyncCommitConsumer", "SCALABILITY_TEST__SYNC_PROCESSING_SYNC_COMMIT_CONSUMER_1");
            //TestScalability("NonBlockingIoSyncProcessingSyncCommitConsumer", "SCALABILITY_TEST__NON_BLOCKING_IO_SYNC_PROCESSING_SYNC_COMMIT_CONSUMER_1");
            //TestScalability("ChunkedPollAsyncProcessingBatchSyncCommitConsumer", "SCALABILITY_TEST__CHUNKED_POLL_ASYNC_PROCESSING_BATCH_SYNC_COMMIT_CONSUMER_1");
            //TestScalability("ChunkedPollAsyncProcessingAsyncCommitConsumer", "SCALABILITY_TEST__CHUNKED_POLL_ASYNC_PROCESSING_ASYNC_COMMIT_CONSUMER_1);


            TestResilienceAsync("AsyncProcessingAutoCommitConsumer", "RESILIENCE_TEST__ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_18").Wait();
            //TestResilienceAsync("ChunkedPollAsyncProcessingAutoCommitConsumer", "RESILIENCE_TEST__CHUNKED_POLL_ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_1").Wait();
            //TestResilienceAsync("AnotherChunkedPollAsyncProcessingAutoCommitConsumer", "RESILIENCE_TEST__ANOTHER_CHUNKED_POLL_ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_6").Wait();
            //TestResilienceAsync("SyncProcessingSyncCommitConsumer", "RESILIENCE_TEST__SYNC_PROCESSING_SYNC_COMMIT_CONSUMER_3").Wait();
            //TestResilienceAsync("NonBlockingIoSyncProcessingSyncCommitConsumer", "RESILIENCE_TEST__NON_BLOCKING_IO_SYNC_PROCESSING_SYNC_COMMIT_CONSUMER_1").Wait();
            //TestResilienceAsync("ChunkedPollAsyncProcessingBatchSyncCommitConsumer", "RESILIENCE_TEST__CHUNKED_POLL_ASYNC_PROCESSING_BATCH_SYNC_COMMIT_CONSUMER_1").Wait();
            //TestResilienceAsync("ChunkedPollAsyncProcessingAsyncCommitConsumer", "RESILIENCE_TEST__CHUNKED_POLL_ASYNC_PROCESSING_ASYNC_COMMIT_CONSUMER_2").Wait();

            












            Console.WriteLine("Done");
            Console.ReadKey();
        }


        static void TestScalability(string consumer, string consumerGroup = null)
        {
            var procCnt = 10;
            var dotnetPath = @"C:\Program Files\dotnet\dotnet.exe";
            var processes = new Process[procCnt];

            var consumerPath = GetConsumerPath(consumer);

            for (var i = 0; i < procCnt; i++)
            {
                processes[i] = System.Diagnostics.Process.Start(dotnetPath, $"{consumerPath} {consumerGroup}");
            }

        }

        static async Task TestResilienceAsync(string consumer, string consumerGroup = null)
        {
            var procCnt = 10;
            var dotnetPath = @"C:\Program Files\dotnet\dotnet.exe";
            var processes = new Process[procCnt];

            var consumerPath = GetConsumerPath(consumer);

            for (var i = 0; i < procCnt; i++)
            {
                //await Task.Delay(30000);
                processes[i] = System.Diagnostics.Process.Start(dotnetPath, $"{consumerPath} {consumerGroup}");
            }

            var remainingProc = procCnt;
            //var rand = new Random(0);
            while (remainingProc > 0)
            {
                var delay = 10000 * (procCnt - remainingProc + 1);
                await Task.Delay(delay);
                //var index = rand.Next(procCnt - 1);
                var index = --remainingProc;


                var proc = processes[index];
                proc.Kill();
                //await Task.Delay(10000);

                //await Task.Delay(30000);
                //processes[index] = System.Diagnostics.Process.Start(dotnetPath, $"{consumerPath} {consumerGroup}");
            }

        }

        static async Task TestResiliencePowerDownPowerUp(string consumer, string consumerGroup = null)
        {
            var procCnt = 10;
            var dotnetPath = @"C:\Program Files\dotnet\dotnet.exe";
            var processes = new Process[procCnt];

            var consumerPath = GetConsumerPath(consumer);

            for (var i = 0; i < procCnt; i++)
            {
                //await Task.Delay(30000);
                processes[i] = System.Diagnostics.Process.Start(dotnetPath, $"{consumerPath} {consumerGroup}");
            }

            await Task.Delay(10000);

            for (var i = 0; i < procCnt; i++)
            {
                //await Task.Delay(30000);
                processes[i] = System.Diagnostics.Process.Start(dotnetPath, $"{consumerPath} {consumerGroup}");
            }

            var remainingProc = procCnt;
            //var rand = new Random(0);
            while (remainingProc > 0)
            {
                var delay = 10000 * (procCnt - remainingProc + 1);
                await Task.Delay(delay);
                //var index = rand.Next(procCnt - 1);
                var index = --remainingProc;


                var proc = processes[index];
                proc.Kill();
                //await Task.Delay(10000);

                //await Task.Delay(30000);
                //processes[index] = System.Diagnostics.Process.Start(dotnetPath, $"{consumerPath} {consumerGroup}");
            }

        }


        private static string GetConsumerPath(string consumer)
        {
            return String.Format("D:\\study\\kafka\\MessageDedup\\{0}\\bin\\Debug\\netcoreapp1.1\\{0}.dll", consumer);
        }
    }
}