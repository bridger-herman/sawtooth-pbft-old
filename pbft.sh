#!/bin/bash

if [ $# -eq 1 ]
then
  docker-compose -f tests/pbft_single.yaml up;
  docker-compose -f tests/pbft_single.yaml down
else
  docker-compose -f tests/pbft_client.yaml up;
  docker-compose -f tests/pbft_client.yaml down
fi
