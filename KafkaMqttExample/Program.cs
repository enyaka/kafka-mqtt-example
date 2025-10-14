using KafkaMqttExample;
using KafkaMqttExample.Services;

var builder = WebApplication.CreateBuilder(args);

builder.AddGraphQL()
    .AddInMemorySubscriptions()
    .AddProjections()
    .AddFiltering()
    .AddSorting()
    .AddQueryType(d => d.Name("Query"))
    .AddTypeExtension<ExampleQuery>()
    .AddMutationType(d => d.Name("Mutation"))
    .AddTypeExtension<ExampleMutation>()
    .AddSubscriptionType(d => d.Name("Subscription"))
    .AddTypeExtension<ExampleSubscription>()
    .AddType<ULongType>()
    .BindRuntimeType<ulong, ULongType>();
    // .AddType<JsonStreamsHeader>()   // <- interface’i implemente eden type’i ekle
    // .AddType<ObservationValueType>()
    // .BindRuntimeType<ObservationValue, ObservationValueType>();

builder.Logging.ClearProviders();
builder.Logging.AddConsole();

// builder.Services.AddHostedService<MqttService>();
builder.Services.AddSingleton<KafkaPartitionConsumerManager>();
builder.Services.AddHostedService<KafkaConsumerService>();
// builder.Services.AddHostedService<KafkaPartitionHostedService>();


var app = builder.Build();
app.UseRouting();
app.UseWebSockets();
app.MapGet("/", () => "MTConnect MQTT -> Kafka bridge is running!");
app.MapGraphQL();
app.Run();