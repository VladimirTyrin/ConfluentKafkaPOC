using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using ConfluentKafkaPOC.Common;

namespace ConfluentKafkaPOC.Producer
{
    public static class Program
    {
        private static Random Random = new Random();

        public static async Task Main(string[] args)
        {
            const int defaultMessageSize = 6000;

            if (args.Length < 2)
            {
                Console.WriteLine("usage:  <SERVERS> <TOPIC> [<SIZE> = 6000]");
                Environment.Exit(1);
            }

            var servers = args[0];
            var topic = args[1];
            var messageSize = defaultMessageSize;
            if (args.Length > 2)
            {
                if (int.TryParse(args[2], out var size) && size > 0)
                    messageSize = size;
                else
                    Console.WriteLine($"Invalid message size: {args[2]}");
            }

            var config = new ProducerConfig
            {
                BootstrapServers = servers
            };

            var cts = new CancellationTokenSource();

            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                cts.Cancel();
                eventArgs.Cancel = true;
            };

            using (var p = new ProducerBuilder<Null, byte[]>(config).Build())
            {
                using (var counter = new RpsCounter())
                {
                    while (!cts.IsCancellationRequested)
                    {
                        try
                        {
                            var message = GenerateMessage(messageSize);
                            await p.ProduceAsync(topic, new Message<Null, byte[]>
                            {
                                Value = message
                            });
                            counter.AddMessage(message.Length);
                        }
                        catch (ProduceException<Null, string> e)
                        {
                            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                    }
                }
            }
        }

        // Generate message of (size +- 10%) bytes
        private static byte[] GenerateMessage(int size)
        {
            var actualSize = (int)(size * ((Random.NextDouble() / 5) + 0.9));

            var result = new byte[actualSize];
            for (var i = 0; i < actualSize; i++)
            {
                result[i] = 0xFF;
            }

            return result;
        }
    }
}
