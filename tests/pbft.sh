#!/bin/bash

source .env

DOCKER_TEST=$SAWTOOTH_CORE/integration/sawtooth_integration/docker/test_pbft_engine.yaml

# docker-compose -f $DOCKER_TEST up --abort-on-container-exit --exit-code-from test-pbft-engine;
docker-compose -f $DOCKER_TEST up;
docker-compose -f $DOCKER_TEST down
