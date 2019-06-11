#!/usr/bin/env bash

set -e

dotnet run --project "src/ConfluentKafkaPOC.V011/ConfluentKafkaPOC.V011.csproj" --configuration Release $@
