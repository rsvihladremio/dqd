#!/bin/bash

GH_TOKEN=$(cat)
./mvnw deploy -Dregistry=https://maven.pkg.github.com/rsvihladremio -Dtoken=$GH_TOKEN
