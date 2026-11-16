#!/bin/sh
set -eu

# Derive URLs at container start so -e LLCONFIG=... takes effect.
LOGGER_ID=${LOGGER_ID:-ait.all.log4j2}
LOGGER_CONFIG="-Dlog4j.configurationFile=${LLCONFIG}${LOGGER_ID}"
CONFIG_FILE_URI="${LLCONFIG}${SYNC_CONFIG}"

# IPv4 configuration.
IPV4_CONFIG=-Djava.net.preferIPv4Stack=true

echo LLCONFIG: $LLCONFIG
echo LOGGER_CONFIG: $LOGGER_CONFIG
echo MEMORY_FLAG: $MEMORY_FLAG
echo SYNC_JAR_FILE: $SYNC_JAR_FILE
echo CONFIG_FILE_URI: $CONFIG_FILE_URI

# Start the sync host. The scenario JSON named in the properties file is read
# from the working directory (/app). Extra arguments are passed through.
exec java $IPV4_CONFIG $LOGGER_CONFIG $MEMORY_FLAG -jar "$SYNC_JAR_FILE" "$CONFIG_FILE_URI" "$@"
