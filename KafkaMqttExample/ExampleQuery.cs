using KafkaMqttExample.Services;

namespace KafkaMqttExample;

[ExtendObjectType(Name = "Query")]
public class ExampleQuery
{
    public string Hello() => "Hello World!";

    public IEnumerable<ConsumerStatus> GetConsumerStatus(
        [Service] KafkaPartitionConsumerManager manager)
    {
        //i
        return manager.GetStatus();
    }
}