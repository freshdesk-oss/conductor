#!/bin/sh
#
#  Copyright 2021 Netflix, Inc.
#  <p>
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
#  <p>
#  http://www.apache.org/licenses/LICENSE-2.0
#  <p>
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
#

# startup.sh - startup script for the server docker image

echo "Starting Conductor server"

function print_log()
{
    echo -e "$(date +'[%F %T %Z]') $*"
}

# Start the server
cd /app/libs
print_log "Property file: $CONFIG_PROP"
print_log $CONFIG_PROP
export config_file=

if [ -z "$CONFIG_PROP" ];
  then
    print_log "Using an in-memory instance of conductor";
    export config_file=/app/config/config-local.properties
  else
    print_log "Using '$CONFIG_PROP'";
    export config_file=/app/config/$CONFIG_PROP
fi

[[ -z ${HOSTNAME} ]] && { print_log "Error: HOSTNAME environment variable not set"; exit 1; }

if [[ "$HOSTNAME" == *"conductor-monitor"* ]]; then
    print_log "Generating Envs for conductor-monitor"
    export WORKFLOW_MONITOR_STATS_INITIAL_DELAY="30000"
    export WORKFLOW_MONITOR_STATS_FIXED_DELAY="10000"
else
    print_log "Generating Envs for common layers"
    export WORKFLOW_MONITOR_STATS_INITIAL_DELAY="-1"
    export WORKFLOW_MONITOR_STATS_FIXED_DELAY="-1"
fi

echo "WORKFLOW_MONITOR_STATS_INITIAL_DELAY: $WORKFLOW_MONITOR_STATS_INITIAL_DELAY"
echo "WORKFLOW_MONITOR_STATS_FIXED_DELAY: $WORKFLOW_MONITOR_STATS_FIXED_DELAY"

echo "Using java options config: $JAVA_OPTS"

OTEL_TRACES_SAMPLER=parentbased_always_off OTEL_RESOURCE_ATTRIBUTES=service.name=${OTEL_SERVICE_NAME},host.name=${POD_NAME},host.ip=${POD_IP} OTEL_EXPORTER_OTLP_ENDPOINT=http://${HOST_IP}:5680 OTEL_METRICS_EXPORTER=none java ${JAVA_OPTS} -jar -DCONDUCTOR_CONFIG_FILE=$config_file conductor-server-*-boot.jar 2>&1 | tee -a /app/logs/server.log
