#!/usr/bin/env bash
# run tests
docker rmi $(docker images -a --filter=dangling=true -q)
docker rm $(docker ps --filter=status=exited --filter=status=created -q)
docker-compose -f docker-compose.test.yml up -d --build --force-recreate
docker logs -f web_test
docker-compose -f docker-compose.test.yml down --remove-orphans
