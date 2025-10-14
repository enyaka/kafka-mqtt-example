using HotChocolate.Language;

namespace KafkaMqttExample;

public class ULongType() : ScalarType<ulong, StringValueNode>("ULong")
{
    protected override ulong ParseLiteral(StringValueNode literal) =>
        ulong.Parse(literal.Value);

    protected override StringValueNode ParseValue(ulong value) =>
        new StringValueNode(value.ToString());

    public override IValueNode ParseResult(object? resultValue) =>
        resultValue is ulong u ? new StringValueNode(u.ToString()) : NullValueNode.Default;
}
