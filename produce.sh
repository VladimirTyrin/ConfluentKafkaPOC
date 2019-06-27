#!/usr/bin/env bash

set -e

dotnet run --project "src/ConfluentKafkaPOC.Producer/ConfluentKafkaPOC.Producer.csproj" --configuration Release $@
