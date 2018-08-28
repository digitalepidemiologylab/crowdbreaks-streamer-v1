#!/usr/bin/env bash

# stop any running docker containers...
sudo docker stop $(sudo docker ps -a -q)

# download binaries...
bash ./scripts/download_binaries.sh

# run docker compose
sudo docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
