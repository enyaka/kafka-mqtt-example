using MTConnect.Streams.Json;

namespace KafkaMqttExample.Dto;

public record DeviceEvent
{
    public DeviceEventHeader Header { get; set; }
    public IEnumerable<DeviceEventStream> Streams { get; set; }

    public DeviceEvent(JsonStreamsDocument streamsDocument)
    {
        Header = new DeviceEventHeader(streamsDocument.Header);
        Streams = streamsDocument.Streams
            .Select(x => new DeviceEventStream(x));
    }
}

public record DeviceEventHeader
{
    public ulong InstanceId { get; set; }

    public string? Version { get; set; }

    public string? Sender { get; set; }

    public ulong BufferSize { get; set; }

    public ulong FirstSequence { get; set; }

    public ulong LastSequence { get; set; }

    public ulong NextSequence { get; set; }

    public string? DeviceModelChangeTime { get; set; }

    public bool TestIndicator { get; set; }

    public DateTime CreationTime { get; set; }

    public DeviceEventHeader(JsonStreamsHeader streamsHeader)
    {
        InstanceId = streamsHeader.InstanceId;
        Version = streamsHeader.Version;
        Sender = streamsHeader.Sender;
        BufferSize = streamsHeader.BufferSize;
        FirstSequence = streamsHeader.FirstSequence;
        LastSequence = streamsHeader.LastSequence;
        NextSequence = streamsHeader.NextSequence;
        DeviceModelChangeTime = streamsHeader.DeviceModelChangeTime;
        TestIndicator = streamsHeader.TestIndicator;
        CreationTime = streamsHeader.CreationTime;
    }
}

public record DeviceEventStream
{
    public string Name { get; set; }

    public string Uuid { get; set; }

    public IEnumerable<DeviceEventComponentStream> ComponentStreams { get; set; }

    public DeviceEventStream(JsonDeviceStream jsonDeviceStream)
    {
        Name = jsonDeviceStream.Name;
        Uuid = jsonDeviceStream.Uuid;
        ComponentStreams = jsonDeviceStream.ComponentStreams?
            .Select(x => new DeviceEventComponentStream(x)) ?? [];
    }
}

public record DeviceEventComponentStream
{
    public string? Component { get; set; }

    public string? ComponentId { get; set; }

    public string? Name { get; set; }

    public string? NativeName { get; set; }

    public string? Uuid { get; set; }

    public IEnumerable<DeviceEventJsonSample> Samples { get; set; }
    public IEnumerable<DeviceEventJsonEvent> Events { get; set; }
    public IEnumerable<DeviceEventJsonCondition> Conditions { get; set; }


    public DeviceEventComponentStream(JsonComponentStream jsonComponentStream)
    {
        Component = jsonComponentStream.Component;
        ComponentId = jsonComponentStream.ComponentId;
        Name = jsonComponentStream.Name;
        NativeName = jsonComponentStream.NativeName;
        Uuid = jsonComponentStream.Uuid;
        Samples = jsonComponentStream.Samples?
            .Select(x => new DeviceEventJsonSample(x)) ?? [];
        Events = jsonComponentStream.Events?
            .Select(x => new DeviceEventJsonEvent(x)) ?? [];
        Conditions = jsonComponentStream.Conditions?
            .Select(x => new DeviceEventJsonCondition(x)) ?? [];
    }
}

public record DeviceEventJsonObservation
{
    public string? DataItemId { get; set; }

    public string? Name { get; set; }

    public string? Category { get; set; }

    public string? Representation { get; set; }

    public string? Type { get; set; }

    public string? SubType { get; set; }

    public string? CompositionId { get; set; }

    public DateTime Timestamp { get; set; }

    public ulong Sequence { get; set; }

    public ulong InstanceId { get; set; }

    public string? ResetTriggered { get; set; }

    public string? Result { get; set; }

    public IEnumerable<string> Samples { get; set; } 

    public IEnumerable<JsonEntry> Entries { get; set; }

    public long? Count { get; set; }

    public string? NativeCode { get; set; }

    public string? AssetType { get; set; }

    public DeviceEventJsonObservation(JsonObservation jsonObservation)
    {
        DataItemId = jsonObservation.DataItemId;
        Name = jsonObservation.Name;
        Category = jsonObservation.Category;
        Representation = jsonObservation.Representation;
        Type = jsonObservation.Type;
        SubType = jsonObservation.SubType;
        CompositionId = jsonObservation.CompositionId;
        Timestamp = jsonObservation.Timestamp;
        Sequence = jsonObservation.Sequence;
        InstanceId = jsonObservation.InstanceId;
        ResetTriggered = jsonObservation.ResetTriggered;
        Result = jsonObservation.Result;
        Samples = jsonObservation.Samples ?? [];
        Entries = jsonObservation.Entries ?? [];
        Count = jsonObservation.Count;
        NativeCode = jsonObservation.NativeCode;
        AssetType = jsonObservation.AssetType;
    }
}

public record DeviceEventJsonSample : DeviceEventJsonObservation
{
    public double? SampleRate { get; set; }

    public string? Statistic { get; set; }

    public double? Duration { get; set; }

    public DeviceEventJsonSample(JsonSample jsonSample) : base(jsonSample)
    {
        SampleRate = jsonSample.SampleRate;
        Statistic = jsonSample.Statistic;
        Duration = jsonSample.Duration;
    }
}

public record DeviceEventJsonEvent : DeviceEventJsonObservation
{
    public DeviceEventJsonEvent(JsonEvent jsonEvent) : base(jsonEvent)
    {
    }
}

public record DeviceEventJsonCondition : DeviceEventJsonObservation
{
    public string? Level { get; set; }

    public string? NativeSeverity { get; set; }

    public string? Qualifier { get; set; }

    public string? Statistic { get; set; }

    public DeviceEventJsonCondition(JsonCondition jsonCondition) : base(jsonCondition)
    {
        Level = jsonCondition.Level;
        NativeSeverity = jsonCondition.NativeSeverity;
        Qualifier = jsonCondition.Qualifier;
        Statistic = jsonCondition.Statistic;
    }
}