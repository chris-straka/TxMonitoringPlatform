docker_build('web-api', '.', dockerfile='src/TxMonitoring.Web.Api/Dockerfile')
docker_build('ingestion-api', '.', dockerfile='src/TxMonitoring.Ingestion.Api/Dockerfile')
docker_build('diagnostics-worker', '.', dockerfile='src/TxMonitoring.Detection.Worker/Dockerfile')
docker_build('sensor-emulator', '.', dockerfile='src/TxMonitoring.Feed.Emulator/Dockerfile')
docker_build('web-dashboard', 'src/TxMonitoring.Web.Dashboard', dockerfile='src/TxMonitoring.Web.Dashboard/Dockerfile')

helm_resource('industrial-platform', 'chart/')
