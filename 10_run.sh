#!/usr/bin/env bash

set -e

dotnet run --project "src/ConfluentKafkaPOC.V10/ConfluentKafkaPOC.V10.csproj" --configuration Release $@
