FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["Directory.Build.props", "./"]
COPY ["src/TxMonitoring.Web.Api/TxMonitoring.Web.Api.csproj", "src/TxMonitoring.Web.Api/"]
COPY ["src/TxMonitoring.Shared/TxMonitoring.Shared.csproj", "src/TxMonitoring.Shared/"]
RUN dotnet restore "src/TxMonitoring.Web.Api/TxMonitoring.Web.Api.csproj"
COPY . .
RUN dotnet publish "src/TxMonitoring.Web.Api/TxMonitoring.Web.Api.csproj" -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "TxMonitoring.Web.Api.dll"]
