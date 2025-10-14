namespace KafkaMqttExample.Services;

public interface IMqttService
{
    Task StartAsync(CancellationToken cancellationToken);
    Task StopAsync(CancellationToken cancellationToken);
}
