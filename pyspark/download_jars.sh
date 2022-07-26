#!/bin/bash

# Download dependencies
mvn dependency:copy-dependencies -DoutputDirectory=jars -Dhttps.protocols=TLSv1.2 -DexcludeScope=provided