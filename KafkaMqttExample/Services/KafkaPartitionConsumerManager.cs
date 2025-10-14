using System.Collections.Concurrent;
using Confluent.Kafka;
using HotChocolate.Subscriptions;

namespace KafkaMqttExample.Services;

public record ConsumerStatus(string Topic, int Partition, bool IsRunning);

public class KafkaPartitionConsumerManager
{
    private readonly ConcurrentDictionary<int, (KafkaPartitionConsumerService Service, Task Task)>
        _consumers = new();


    public void StartConsumers(IEnumerable<TopicPartition> partitions, ConsumerConfig baseConfig,
        ITopicEventSender sender)
    {
        foreach (var partition in partitions)
        {
            if (_consumers.ContainsKey(partition.Partition.Value))
            {
                // Console.WriteLine("Consumer for partition {Partition} already running", partition);
                Console.WriteLine($"Consumer for partition {partition} already running");
                continue;
            }

            var service = new KafkaPartitionConsumerService(baseConfig, partition, sender, 1000);
            var task = Task.Run(async () => await service.RunConsumeLoopAsync());

            _consumers[partition.Partition.Value] = (service, task);
            Console.WriteLine($"Started consumer for {partition}");
        }
    }

    public IEnumerable<TopicPartition> StartExistingConsumers(IEnumerable<int> partitions)
    {
        foreach (var partition in partitions)
        {
            if (!_consumers.TryGetValue(partition, out var consumer)) continue;
            if (!consumer.Task.IsCompleted)
            {
                Console.WriteLine($"Consumer for partition {partition} already running");
                continue;
            }

            var task = Task.Run(() => consumer.Service.RunConsumeLoopAsync());
            _consumers[partition] = (consumer.Service, task);
            Console.WriteLine($"Started consumer for {consumer.Service.Partition}");
            yield return consumer.Service.Partition;
        }
    }

    public async IAsyncEnumerable<TopicPartition> StopConsumers(IEnumerable<int> partitions)
    {
        foreach (var partition in partitions)
        {
            if (!_consumers.TryGetValue(partition, out var consumer)) continue;
            await consumer.Service.StopAsync();
            Console.WriteLine($"Stopped consumer for {partition}");
            yield return consumer.Service.Partition;
        }
    }

    public async IAsyncEnumerable<TopicPartition> StopAndRemoveConsumers(IEnumerable<int> partitions)
    {
        foreach (var partition in partitions)
        {
            if (!_consumers.TryRemove(partition, out var consumer)) continue;
            await consumer.Service.StopAsync();
            Console.WriteLine($"Stopped consumer for {partition}");
            yield return consumer.Service.Partition;
        }
    }

    public async IAsyncEnumerable<TopicPartition> StopAllConsumers()
    {
        foreach (var consumer in _consumers)
        {
            await consumer.Value.Service.StopAsync();
            Console.WriteLine($"Stopped consumer for {consumer.Key}");
            yield return consumer.Value.Service.Partition;
        }
    }

    public async IAsyncEnumerable<TopicPartition> StopAndRemoveAllConsumers()
    {
        foreach (var consumer in _consumers)
        {
            await consumer.Value.Service.StopAsync();
            Console.WriteLine($"Stopped consumer for {consumer.Key}");
            _consumers.TryRemove(consumer.Key, out _);
            yield return consumer.Value.Service.Partition;
        }
    }

    public IEnumerable<ConsumerStatus> GetStatus()
    {
        return _consumers.Select(x =>
            new ConsumerStatus(x.Value.Service.Partition.Topic, x.Key,
                !x.Value.Task.IsCompleted));
    }
}