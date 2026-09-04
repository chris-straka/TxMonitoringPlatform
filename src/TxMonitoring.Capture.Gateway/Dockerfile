FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["Directory.Build.props", "./"]
COPY ["src/TxMonitoring.Capture.Gateway/TxMonitoring.Capture.Gateway.csproj", "src/TxMonitoring.Capture.Gateway/"]
COPY ["src/TxMonitoring.Shared/TxMonitoring.Shared.csproj", "src/TxMonitoring.Shared/"]
COPY ["src/Protos/telemetry.proto", "src/Protos/"]
RUN dotnet restore "src/TxMonitoring.Capture.Gateway/TxMonitoring.Capture.Gateway.csproj"
COPY . .
RUN dotnet publish "src/TxMonitoring.Capture.Gateway/TxMonitoring.Capture.Gateway.csproj" -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app

# The aspnet image ships no HTTP client, so the compose healthcheck has nothing to call
# A bash /dev/tcp probe would prove the port is open but not that Kestrel answers
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# The buffer lives on a mounted volume, NOT in the image layer. If it lived in the
# container filesystem, `docker compose restart edge-gateway` would silently wipe the
# queue -- and "restart the gateway mid-outage and watch it resume" is the demo.
VOLUME ["/data"]
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "TxMonitoring.Capture.Gateway.dll"]
