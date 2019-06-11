using System.Collections.Generic;
using Confluent.Kafka.Serialization;

namespace ConfluentKafkaPOC.V011
{
    public class NoOpDeserializer : IDeserializer<byte[]>
    {
        public void Dispose()
        {
            
        }

        public byte[] Deserialize(string topic, byte[] data)
        {
            return data;
        }

        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            return config;
        }
    }
}