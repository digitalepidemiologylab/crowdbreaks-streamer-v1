#!/usr/bin/env bash

# stop any running docker containers...
sudo docker stop $(sudo docker ps -a -q)

# run docker compose
sudo docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
