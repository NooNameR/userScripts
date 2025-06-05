#!/bin/bash
curl --silent -G -u "$AUTOPULSE_USERNAME:$AUTOPULSE_PASSWORD" "http://autopulse:2875/triggers/manual" --data-urlencode "path=$1"