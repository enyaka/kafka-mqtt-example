using System.Text.Json.Serialization;

namespace KafkaMqttExample.Dto;

public class EventHeader
{
    [JsonPropertyName("topic")] public string? Topic { get; init; }
    [JsonPropertyName("id")] public string? Id { get; init; } 
    public string? DeviceId => Topic?.Split("/").LastOrDefault();
}