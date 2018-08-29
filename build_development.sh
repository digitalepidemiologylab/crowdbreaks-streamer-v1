#!/usr/bin/env bash

# This is a somewhat paranoid way to build containers when a lot of of old containers are lingering around.
# Otherwise, a simple `docker-compose up` should do the trick...

# stop any running docker containers...
docker stop $(docker ps -a -q)

# remove any dangling images
docker rmi $(docker images -a --filter=dangling=true -q)
docker rm $(docker ps --filter=status=exited --filter=status=created -q)

# build and start
docker-compose up --build --remove-orphans
