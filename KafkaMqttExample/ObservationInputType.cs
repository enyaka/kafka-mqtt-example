using MTConnect.Input;

namespace KafkaMqttExample;

public class ObservationInputType : InputObjectType<ObservationInput>
{
    protected override void Configure(IInputObjectTypeDescriptor<ObservationInput> descriptor)
    {
        descriptor.Name("ObservationInput"); // GraphQL şemada görünen isim
        // descriptor.Field(f => f.Value).Type<NonNullType<LongType>>();
        // ulong desteklenmediği için LongType ya da custom scalar kullan
    }
}