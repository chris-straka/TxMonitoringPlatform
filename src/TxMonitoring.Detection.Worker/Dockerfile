FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["Directory.Build.props", "./"]
COPY ["src/TxMonitoring.Detection.Worker/TxMonitoring.Detection.Worker.csproj", "src/TxMonitoring.Detection.Worker/"]
COPY ["src/TxMonitoring.Shared/TxMonitoring.Shared.csproj", "src/TxMonitoring.Shared/"]
RUN dotnet restore "src/TxMonitoring.Detection.Worker/TxMonitoring.Detection.Worker.csproj"

COPY . .
RUN dotnet publish "src/TxMonitoring.Detection.Worker/TxMonitoring.Detection.Worker.csproj" -c Release -o /out --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out .

ENTRYPOINT ["dotnet", "TxMonitoring.Detection.Worker.dll"]
