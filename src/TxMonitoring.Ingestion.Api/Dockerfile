FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["Directory.Build.props", "./"]
COPY ["src/TxMonitoring.Ingestion.Api/TxMonitoring.Ingestion.Api.csproj", "src/TxMonitoring.Ingestion.Api/"]
COPY ["src/TxMonitoring.Shared/TxMonitoring.Shared.csproj", "src/TxMonitoring.Shared/"]
COPY ["src/Protos/telemetry.proto", "src/Protos/"]
RUN dotnet restore "src/TxMonitoring.Ingestion.Api/TxMonitoring.Ingestion.Api.csproj"
COPY . .
RUN dotnet publish "src/TxMonitoring.Ingestion.Api/TxMonitoring.Ingestion.Api.csproj" -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "TxMonitoring.Ingestion.Api.dll"]
