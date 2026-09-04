using Microsoft.Extensions.Configuration;

using TxMonitoring.Ingestion.Api.Configuration;

namespace TxMonitoring.Ingestion.Tests;

public sealed class GrpcOptionsTests
{
    [Fact]
    public void MissingSectionFallsBackToCodeDefaults()
    {
        var configuration = new ConfigurationBuilder().Build();

        var options = GrpcOptions.Read(configuration);

        Assert.Equal(GrpcOptions.DefaultMaxReceiveMessageSizeBytes, options.MaxReceiveMessageSizeBytes);
    }

    [Fact]
    public void PresentSectionBindsOverrides()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Grpc:MaxReceiveMessageSizeBytes"] = "1048576",
            })
            .Build();

        var options = GrpcOptions.Read(configuration);

        Assert.Equal(1_048_576, options.MaxReceiveMessageSizeBytes);
    }
}
