# Build images (2nd arg is dockerfile build context, usually dotnet restore)
docker_build('ingestion-api', '.', dockerfile='src/TxMonitoring.Ingestion.Api/Dockerfile')
docker_build('diagnostics-worker', '.', dockerfile='src/TxMonitoring.Detection.Worker/Dockerfile')
docker_build('sensor-emulator', '.', dockerfile='src/TxMonitoring.Feed.Emulator/Dockerfile')
docker_build('web-api', '.', dockerfile='src/TxMonitoring.Web.Api/Dockerfile')

# Development uses node:24, production uses my dockerfile
# Dashboard only needs the package.json for the build context
# docker_build('web-dashboard', 'src/TxMonitoring.Web.Dashboard', dockerfile='src/TxMonitoring.Web.Dashboard/Dockerfile')

# Spin up Postgres and Kafka
docker_compose('docker-compose.yaml')
