$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$module = Join-Path $root "jobs/datastream-quality"
$jar = Join-Path $module "target/datastream-quality-1.0.0.jar"

Write-Host "Testing and building Chapter 9 with Java 17..."
docker run --rm `
    --volume "${root}:/workspace" `
    --volume "ecommerce-maven-cache:/root/.m2" `
    --workdir "/workspace/jobs/datastream-quality" `
    maven:3.9.9-eclipse-temurin-17 `
    mvn -q clean test package
if ($LASTEXITCODE -ne 0) {
    throw "Chapter 9 Maven test or build failed."
}
if (-not (Test-Path -LiteralPath $jar)) {
    throw "Fat JAR was not generated: $jar"
}

Write-Host "Fat JAR generated: $jar"
