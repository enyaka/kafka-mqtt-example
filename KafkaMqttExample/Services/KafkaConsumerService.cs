using System.Text.Json;
using System.Threading.Channels;
using Confluent.Kafka;
using HotChocolate.Subscriptions;
using KafkaMqttExample.Dto;
using MTConnect.Streams;
using MTConnect.Streams.Json;
using MTConnect.Streams.Output;

namespace KafkaMqttExample.Services;

public class KafkaConsumerService : BackgroundService
{
    private const string KafkaTopic = "mqtt_topic"; // Yeni topic
    private const string KafkaGroupId = "webapi-consumer-group";
    private const string BootstrapServers = "localhost:7001";

    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly ITopicEventSender _sender;
    private readonly IConsumer<string, string> _consumer;
    private CancellationTokenSource? _cancellationTokenSource;
    private short CommitCount { get; }
    private readonly List<JsonStreamsDocument> _streamList;


    public KafkaConsumerService(ILogger<KafkaConsumerService> logger, ITopicEventSender sender)
    {
        _logger = logger;
        _sender = sender;
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = KafkaGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false, // offset’leri otomatik commit etmek icin true
            EnableAutoOffsetStore = false,
            FetchMaxBytes = 50 * 1024 * 1024, // 50 MB
            MaxPartitionFetchBytes = 50 * 1024 * 1024
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        CommitCount = 1000;
        _streamList = new List<JsonStreamsDocument>(CommitCount);
    }


    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        _consumer.Subscribe(KafkaTopic);
        Console.WriteLine(
            $" Consumer Loop started for {KafkaTopic}. Assigned Thread Id: {Environment.CurrentManagedThreadId}. Count set to: {CommitCount}");
        await Task.Delay(1000, stoppingToken);
        try
        {
            while (!_cancellationTokenSource.IsCancellationRequested)
            {
                var cr = _consumer?.Consume(_cancellationTokenSource.Token);
                if (string.IsNullOrWhiteSpace(cr?.Message.Value) || string.IsNullOrWhiteSpace(cr.Message.Key))
                {
                    _consumer?.StoreOffset(cr);
                    continue;
                }

                try
                {
                    var response = JsonSerializer.Deserialize<JsonStreamsDocument>(cr.Message.Value);
                    var key = JsonSerializer.Deserialize<EventHeader>(cr.Message.Key);

                    if (response is not null)
                    {
                        _streamList.Add(response);
                        _consumer?.StoreOffset(cr);
                        var deviceEvent = new DeviceEvent(response);
                        await _sender.SendAsync("device_event_added", deviceEvent, _cancellationTokenSource.Token);
                        if (!string.IsNullOrWhiteSpace(key?.DeviceId))
                            await _sender.SendAsync(key.DeviceId, deviceEvent, _cancellationTokenSource.Token);
                        if (_streamList.Count >= CommitCount)
                        {
                            if (Random.Shared.Next(0, 100) < 10)
                                throw new Exception("Random error");
                            var committedTopicOffsets = _consumer?.Commit();
                            Console.WriteLine(
                                $"[{DateTime.Now}] ThreadId: {Environment.CurrentManagedThreadId}. Topic {KafkaTopic}. Commited event count: {_streamList.Count}");
                            committedTopicOffsets?.ForEach(x =>
                                Console.WriteLine(
                                    $"Committed offset: {x.Offset} for partition {x.Partition}"));
                            Console.WriteLine();
                            _streamList.Clear();
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error processing message: {e}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine($"Stopped consuming {KafkaTopic}");
            _consumer?.Close();
            if (!_cancellationTokenSource.IsCancellationRequested)
                await _cancellationTokenSource.CancelAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            _consumer?.Close();
            if (!_cancellationTokenSource.IsCancellationRequested)
                await _cancellationTokenSource.CancelAsync();
        }
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _cancellationTokenSource?.Cancel();
        _consumer.Close();
        _consumer.Dispose();
        _logger.LogInformation("KafkaConsumerService stopped");
        return Task.CompletedTask;
    }
}