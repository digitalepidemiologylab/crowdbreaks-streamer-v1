sudo docker stop $(sudo docker ps -a -q)
sudo docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
