using KafkaMqttExample.Dto;

namespace KafkaMqttExample;

[ExtendObjectType(Name = "Subscription")]
public class ExampleSubscription
{
    [Subscribe]
    [Topic("device_event_added")]
    public DeviceEvent DeviceEventAdded([EventMessage] DeviceEvent deviceEvent)
    {
        return deviceEvent;
    }

    [Subscribe]
    [Topic($"{{{nameof(deviceId)}}}")]
    public DeviceEvent DeviceEventAddedById(string deviceId, [EventMessage] DeviceEvent deviceEvent)
    {
        return deviceEvent;
    }
}