using Confluent.Kafka;
using KafkaMqttExample.Services;

namespace KafkaMqttExample;

[ExtendObjectType(Name = "Mutation")]
public class ExampleMutation
{
    public IEnumerable<TopicPartition> StartExistingConsumers([Service] KafkaPartitionConsumerManager manager,
        IEnumerable<int> partitions)
    {
        return manager.StartExistingConsumers(partitions);
    }


    public IAsyncEnumerable<TopicPartition> StopConsumers([Service] KafkaPartitionConsumerManager manager,
        IEnumerable<int> partitions)
    {
        return manager.StopConsumers(partitions);
    }

    public IAsyncEnumerable<TopicPartition> StopAllConsumers([Service] KafkaPartitionConsumerManager manager)
    {
        return manager.StopAllConsumers();
    }
}