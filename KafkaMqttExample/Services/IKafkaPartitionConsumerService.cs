namespace KafkaMqttExample.Services;

public interface IKafkaPartitionConsumerService
{
    Task RunConsumeLoopAsync();
    Task StopAsync();
}