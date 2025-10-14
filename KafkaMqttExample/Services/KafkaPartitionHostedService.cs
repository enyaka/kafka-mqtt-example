using Confluent.Kafka;
using HotChocolate.Subscriptions;

namespace KafkaMqttExample.Services;

public class KafkaPartitionHostedService
    : IHostedService
{
    private const string KafkaTopic = "mqtt_topic"; // Yeni topic
    private const string KafkaGroupId = "webapi-consumer-group";
    private const string BootstrapServers = "localhost:7001";

    private readonly CancellationTokenSource _cts = new();
    private readonly ITopicEventSender _sender;
    private readonly ILogger<KafkaPartitionHostedService> _logger;
    private readonly ConsumerConfig _initialConsumerConfig;
    private readonly KafkaPartitionConsumerManager _manager;

    public KafkaPartitionHostedService(ILogger<KafkaPartitionHostedService> logger, ITopicEventSender sender,
        KafkaPartitionConsumerManager manager)
    {
        _logger = logger;
        _sender = sender;
        _manager = manager;
        _initialConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = KafkaGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            FetchMaxBytes = 50 * 1024 * 1024, // 50 MB
            MaxPartitionFetchBytes = 50 * 1024 * 1024,
            //Initial olduğundan consume edilen eventin commit edilmemesi lazım. Bu görev partition consumerların işi
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
        };
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // var manager = new KafkaPartitionConsumerManager();
        using var consumer = new ConsumerBuilder<string, string>(_initialConsumerConfig)
            .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                Console.WriteLine($"Assigned: {string.Join(", ", partitions)}");
                var partitionConsumerConfig = new ConsumerConfig
                {
                    BootstrapServers = BootstrapServers,
                    GroupId = KafkaGroupId,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    FetchMaxBytes = 50 * 1024 * 1024, // 50 MB
                    MaxPartitionFetchBytes = 50 * 1024 * 1024,
                    EnableAutoCommit = false,
                    EnableAutoOffsetStore = false,
                };
                _manager.StartConsumers(partitions, partitionConsumerConfig, _sender);
                // foreach (var partitionConsumerService in from topicPartition in partitions
                //          let partitionConsumerConfig = new ConsumerConfig
                //          {
                //              BootstrapServers = BootstrapServers,
                //              GroupId = KafkaGroupId,
                //              AutoOffsetReset = AutoOffsetReset.Earliest,
                //              FetchMaxBytes = 50 * 1024 * 1024, // 50 MB
                //              MaxPartitionFetchBytes = 50 * 1024 * 1024,
                //              EnableAutoCommit = false,
                //              EnableAutoOffsetStore = false,
                //          }
                //          select new KafkaPartitionConsumerService(partitionConsumerConfig, topicPartition, _sender, 1000))
                // {
                //     Task.Run(() => partitionConsumerService.RunConsumeLoopAsync(cancellationToken));
                // }
                // foreach (var partition in partitions)
                //     Task.Run(() => ConsumePartition(partition, cancellationToken), cancellationToken);

                _cts.Cancel();
            })
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                Console.WriteLine($"Revoked: {string.Join(", ", partitions)}");
            })
            .Build();

        consumer.Subscribe(KafkaTopic);
        try
        {
            consumer.Consume(_cts.Token);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }
        finally
        {
            consumer.Close();
        }


        await Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await Task.CompletedTask;
    }


    // private async Task ConsumePartition(TopicPartition partition, CancellationToken stoppingToken)
    // {
    //     var consumerConfig = new ConsumerConfig
    //     {
    //         BootstrapServers = BootstrapServers,
    //         GroupId = KafkaGroupId,
    //         AutoOffsetReset = AutoOffsetReset.Earliest,
    //         FetchMaxBytes = 50 * 1024 * 1024, // 50 MB
    //         MaxPartitionFetchBytes = 50 * 1024 * 1024,
    //         EnableAutoCommit = false,
    //         EnableAutoOffsetStore = false,
    //     };
    //     using var consumer = new ConsumerBuilder<string, string>(consumerConfig)
    //         .Build();
    //
    //     consumer?.Assign(partition);
    //     Console.WriteLine($"Started consuming {partition}");
    //     var streamList = new List<JsonStreamsDocument>();
    //
    //     try
    //     {
    //         while (!stoppingToken.IsCancellationRequested)
    //         {
    //             var cr = consumer?.Consume(stoppingToken);
    //             // Console.WriteLine(
    //             //     $"Partition {partition.Partition}: {cr?.Message.Value}");
    //             if (string.IsNullOrEmpty(cr?.Message.Value))
    //             {
    //                 consumer?.StoreOffset(cr);
    //                 continue;
    //             }
    //
    //             try
    //             {
    //                 var response = JsonSerializer.Deserialize<JsonStreamsDocument>(cr.Message.Value);
    //                 if (response is not null)
    //                 {
    //                     // Console.WriteLine(
    //                     //     $"Partition {partition.Partition}: {cr.Message.Key} \n Value: {response.Streams.FirstOrDefault()?.Uuid}");
    //                     streamList.Add(response);
    //                     // Console.WriteLine($"Partition {partition.Partition.Value} => Stored offset: {cr.Offset}, StreamList Count {streamList.Count} \n");
    //                     consumer?.StoreOffset(cr);
    //                     var deviceEvent = new DeviceEvent(response);
    //                     await sender.SendAsync("device_event_added", deviceEvent, stoppingToken);
    //                     if (streamList.Count == 10)
    //                     {
    //                         streamList.Clear();
    //                         var committedTopicOffsets = consumer?.Commit();
    //                         committedTopicOffsets?.ForEach(x =>
    //                             Console.WriteLine(
    //                                 $"*********\nCommitted offset: {x.Offset} for partition {x.Partition} and topic {x.Topic}\n********\n"));
    //                     }
    //                 }
    //             }
    //             catch (Exception e)
    //             {
    //                 Console.WriteLine(e);
    //             }
    //
    //             // Console.WriteLine(
    //             //     $"Partition {partition.Partition}: {cr?.Message.Key} \n Value: {cr?.Message.Value}");
    //             // Console.WriteLine(
    //             //     $"Partition {partition.Partition}: {cr?.Message.Key}");
    //         }
    //     }
    //     catch (OperationCanceledException)
    //     {
    //         Console.WriteLine($"Stopped consuming {partition}");
    //     }
    // }
}