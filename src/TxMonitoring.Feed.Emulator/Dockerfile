FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["Directory.Build.props", "./"]
COPY ["src/TxMonitoring.Feed.Emulator/TxMonitoring.Feed.Emulator.csproj", "src/TxMonitoring.Feed.Emulator/"]
COPY ["src/TxMonitoring.Shared/TxMonitoring.Shared.csproj", "src/TxMonitoring.Shared/"]
RUN dotnet restore "src/TxMonitoring.Feed.Emulator/TxMonitoring.Feed.Emulator.csproj"
COPY . .
# -c is for C# compiler build optimizations and not the environment
RUN dotnet publish "src/TxMonitoring.Feed.Emulator/TxMonitoring.Feed.Emulator.csproj" -c Release -o /app/publish --no-restore

# runtime, not aspnet: this image needs the .NET runtime but never the ASP.NET Core
# shared framework, because nothing here listens on a port. Smaller image, less surface.
FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "TxMonitoring.Feed.Emulator.dll"]
