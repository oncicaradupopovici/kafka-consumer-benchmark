using System;
using System.Collections.Generic;
using System.Numerics;
using System.Text;

namespace Business
{
    public class Message
    {
        public Message(string value, string key, string consumerGroup, string consumerId, string topic, int partition, long offset)
        {
            this.Value = value;
            this.Key = key;
            this.ConsumerGroup = consumerGroup;
            this.ConsumerId = consumerId;
            this.Topic = topic;
            this.Partition = partition;
            this.Offset = offset;
        }

        public string Value { get;}
        public string ConsumerId { get; }
        public string ConsumerGroup { get; }

        public string Topic { get; }

        public int Partition { get; }

        public long Offset { get; }

        public string Key { get; }
    }
}
