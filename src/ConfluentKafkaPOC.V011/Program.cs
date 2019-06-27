using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using ConfluentKafkaPOC.Common;

namespace ConfluentKafkaPOC.V011
{
    public static class Program
    {
        private static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("usage:  <SERVERS> <TOPIC>");
                Environment.Exit(1);
            }

            var servers = args[0];
            var topic = args[1];

            var settings = new Dictionary<string, object>
            {
                ["bootstrap.servers"] = servers,
                ["group.id"] = Guid.NewGuid().ToString("N").Substring(0, 10),
                ["api.version.request"] = true,
                ["socket.blocking.max.ms"] = 5,
                ["queue.buffering.max.ms"] = 100,
                ["enable.auto.commit"] = true,
                ["fetch.wait.max.ms"] = 100,
                ["fetch.error.backoff.ms"] = 5,
                ["fetch.message.max.bytes"] = 10240,
                ["queued.min.messages"] = 1000,
                ["heartbeat.interval.ms"] = 2000,
                ["auto.offset.reset"] = "smallest",
                ["enable.auto.offset.store"] = false,
                ["auto.commit.interval.ms"] = 1000,
                ["fetch.min.bytes"] = 10000,
                ["statistics.interval.ms"] = 60000,
                ["session.timeout.ms"] = 6000,
                ["log_level"] = 7
            };

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                eventArgs.Cancel = true;
                cts.Cancel();
            };

            using (var consumer =
                new Consumer<Ignore, byte[]>(settings, new IgnoreDeserializer(), new NoOpDeserializer()))
            {
                using (var counter = new RpsCounter())
                {
                    consumer.Subscribe(topic);

                    while (! cts.IsCancellationRequested)
                    {
                        if (consumer.Consume(out var message, 5000))
                        {
                            counter.AddMessage(message.Value.Length);
                            consumer.StoreOffset(message);
                        }
                    }

                    consumer.Unsubscribe();
                }
            }
        }
    }
}
