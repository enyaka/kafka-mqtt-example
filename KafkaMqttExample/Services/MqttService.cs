using MQTTnet;
using Confluent.Kafka;
using System.Text;

namespace KafkaMqttExample.Services;

public class MqttService : BackgroundService
{
    private readonly ILogger<MqttService> _logger;
    private readonly IProducer<string, string> _kafkaProducer;
    private IMqttClient? _mqttClient;

    public MqttService(ILogger<MqttService> logger)
    {
        _logger = logger;

        // Kafka producer config
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092,localhost:9094,localhost:9096"
        };
        _kafkaProducer = new ProducerBuilder<string, string>(config).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var mqttFactory = new MqttClientFactory();
        _mqttClient = mqttFactory.CreateMqttClient();

        var mqttOptions = new MqttClientOptionsBuilder()
            .WithTcpServer("localhost", 1883) // MQTT relay host/port
            .WithClientId("dotnet-mqtt-client")
            .Build();

        _mqttClient.ApplicationMessageReceivedAsync += async e =>
        {
            try
            {
                var payloadBytes = e.ApplicationMessage.Payload;
                var message = Encoding.UTF8.GetString(payloadBytes);

                _logger.LogInformation("MQTT message received: {Message}", message);

                // Kafka publish
                await _kafkaProducer.ProduceAsync("mtconnect-data", new Message<string, string>
                {
                    Key = e.ApplicationMessage.Topic,
                    Value = message
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing MQTT message");
            }
        };

        _mqttClient.ConnectedAsync += e =>
        {
            _logger.LogInformation("Connected to MQTT broker");
            return Task.CompletedTask;
        };

        _mqttClient.DisconnectedAsync += e =>
        {
            _logger.LogWarning("Disconnected from MQTT broker");
            return Task.CompletedTask;
        };

        await _mqttClient.ConnectAsync(mqttOptions, stoppingToken);

        // Subscribe to topics (örnek: tüm agent dataları)
        await _mqttClient.SubscribeAsync("MTConnect/Document/#", cancellationToken: stoppingToken);

        // Keep alive loop
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken);
        }
    }

    public override async Task StopAsync(CancellationToken stoppingToken)
    {
        if (_mqttClient != null)
        {
            await _mqttClient.DisconnectAsync(cancellationToken: stoppingToken);
        }

        _kafkaProducer.Flush(TimeSpan.FromSeconds(5));
        _kafkaProducer.Dispose();
        await base.StopAsync(stoppingToken);
    }
}