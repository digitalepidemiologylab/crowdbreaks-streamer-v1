#!/usr/bin/env bash

# This is a somewhat paranoid way to build containers when a lot of of old containers are lingering around.
# Otherwise, a simple `docker-compose up` should do the trick...

# stop any running docker containers...
sudo docker stop $(sudo docker ps -a -q)

# remove any dangling images
sudo docker rmi $(sudo docker images -q -f dangling=true)

# build and start
docker-compose up --build --remove-orphans
