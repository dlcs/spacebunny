#!/bin/bash

docker run \
        --env BUNNY_AWS_REGION="eu-west-1"  \
        --env AWS_ACCESS_KEY_ID="CHANGEME" \
        --env AWS_SECRET_ACCESS_KEY="CHANGEME"  \
        --env BUNNY_INPUT_QUEUE=bunny-input  \
        --env BUNNY_ERROR_QUEUE=bunny-input  \
        --env BUNNY_NOTIFICATION_QUEUE=bunny-notification  \
        --env BUNNY_RESPONSE_QUEUE=bunny-response  \
        --env BUNNY_PIPELINE=bunny-pipeline  \
        --env BUNNY_OUTPUT_BUCKET=bunny-output  \
        spacebunny