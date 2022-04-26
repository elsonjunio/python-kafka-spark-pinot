#!/bin/bash
TAG=$(date | md5sum | awk '{ print $1 }')

# Download dependencies
mvn dependency:copy-dependencies -DoutputDirectory=jars -Dhttps.protocols=TLSv1.2 -DexcludeScope=provided