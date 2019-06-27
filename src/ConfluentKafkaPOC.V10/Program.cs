using System;
using System.Threading;
using Confluent.Kafka;
using ConfluentKafkaPOC.Common;

namespace ConfluentKafkaPOC.V10
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("usage:  <SERVERS> <TOPIC>");
                Environment.Exit(1);
            }

            var servers = args[0];
            var topic = args[1];

            var config = new ConsumerConfig
            {
                EnableAutoCommit = true,
                BootstrapServers = servers,
                GroupId = Guid.NewGuid().ToString("N").Substring(0, 10),
                AutoCommitIntervalMs = 1000
            };

            var builder = new ConsumerBuilder<Ignore, byte[]>(config);

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                eventArgs.Cancel = true;
                cts.Cancel();
            };

            using (var consumer = builder.Build())
            {
                consumer.Subscribe(topic);

                using (var counter = new RpsCounter())
                {
                    while (!cts.IsCancellationRequested)
                    {
                        try
                        {
                            var result = consumer.Consume(cts.Token);
                            counter.AddMessage(result.Message.Value.Length);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                    }
                }

                consumer.Unsubscribe();
            }
        }
    }
}
