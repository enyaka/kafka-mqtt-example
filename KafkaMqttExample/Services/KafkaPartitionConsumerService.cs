using System.Text.Json;
using Confluent.Kafka;
using HotChocolate.Subscriptions;
using KafkaMqttExample.Dto;
using MTConnect.Streams.Json;

namespace KafkaMqttExample.Services;

public class KafkaPartitionConsumerService : IKafkaPartitionConsumerService
{
    private readonly IConsumer<string, string> _consumer;
    public TopicPartition Partition { get; }
    private readonly ITopicEventSender _sender;
    private readonly List<JsonStreamsDocument> _streamList;
    private short CommitCount { get; }
    private CancellationTokenSource _cancellationTokenSource;

    public KafkaPartitionConsumerService(
        ConsumerConfig consumerConfig,
        TopicPartition partition,
        ITopicEventSender sender, short commitCount = 10)
    {
        _consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .Build();
        _consumer.Assign(partition);
        Partition = partition;
        _sender = sender;
        CommitCount = commitCount;
        _cancellationTokenSource = new CancellationTokenSource();
        _streamList = new List<JsonStreamsDocument>(commitCount);
    }

    public async Task RunConsumeLoopAsync()
    {
        Console.WriteLine(
            $"Partition Consumer Loop started for {Partition}. Assigned Thread Id: {Environment.CurrentManagedThreadId}. Count set to: {CommitCount}");
        if (_cancellationTokenSource.IsCancellationRequested)
            _cancellationTokenSource = new CancellationTokenSource();
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
                        if (_streamList.Count == CommitCount)
                        {
                            var committedTopicOffsets = _consumer?.Commit();
                            committedTopicOffsets?.ForEach(x =>
                                Console.WriteLine(
                                    $"[{DateTime.Now}] ThreadId: {Environment.CurrentManagedThreadId} Committed offset: {x.Offset} for partition {x.Partition} and topic {x.Topic}. Commited event count: {_streamList.Count}"));
                            _streamList.Clear();
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error processing message: {e}");
                    await Task.Delay(2000);
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine($"Stopped consuming {Partition}");
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

    public async Task StopAsync() => await _cancellationTokenSource.CancelAsync();
}