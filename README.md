# Crowdbreaks Flask API 

Intended to be used for stream processing (currently from logstash), running ML tasks (such as vaccine sentiments), and forwarding them to ElasticSearch.


# Installation
Setting up development environment:
## Flask
```
git clone git@github.com:salathegroup/crowdbreaks-flask-api.git
cd crowdbreaks-flask-api
conda env create -f environment.yml
source activate crowdbreaks-flask

# start server
export FLASK_APP=app.py
export FLASK_DEBUG=1
flask run
# or without CLI just using
python app.py
```
Set up configuration:

* default keys: `config.py`

* secret keys: `instance/config.conf`
```
mkdir instance
cp config.py.example ./instance/config.py
# Set secret keys accordingly...
```


## Redis
On MacOSX:
```
brew install redis
# start redis-server using config file 
redis-server /usr/local/etc/redis.conf

# or using brew services
brew services start redis
# logfile under: /usr/local/var/log/redis.log
# config file: /usr/local/etc/redis.conf
# data: /usr/local/var

```

## Production EC2 (Ubuntu 16.04)
Install anaconda
```
cd && mkdir downloads && cd downloads
wget https://repo.continuum.io/archive/Anaconda3-5.0.1-Linux-x86_64.sh
bash Anaconda3-5.0.1-Linux-x86_64.sh -b
```
### Pull repo
```
cd && git clone git clone https://github.com/salathegroup/crowdbreaks-flask-api.git && cd crowdbreaks-flask-api/
conda create --name flask-api
source activate flask-api
pip install -r requirements.txt
mkdir instance
cp config.py.example instance/config.py
# Add secrets to instance/config.py

# Download SVM binaries
cd ~/crowdbreaks-flask-api/bin/vaccine_sentiment/
wget https://s3.eu-central-1.amazonaws.com/crowdbreaks-dev/binaries/sent2vec_v1.0.p
``` 
### Install Redis
Mostly following [this](https://www.digitalocean.com/community/tutorials/how-to-install-and-configure-redis-on-ubuntu-16-04)
```
sudo apt-get update
sudo apt-get install build-essential tcl
cd /tmp
curl -O http://download.redis.io/redis-stable.tar.gz
wget http://download.redis.io/redis-stable.tar.gz
tar xzvf redis-stable.tar.gz
cd redis-stable
make
sudo make install
```
### Configure and run Redis
```
sudo mkdir /etc/redis
sudo cp ~/crowdbreaks-flask-api/lib/configs/redis.conf /etc/redis
# Set password under requirepass 
# Note that this config is using port 6389

# Create service
sudo cp ~/crowdbreaks-flask-api/lib/configs/redis.service /etc/systemd/system/redis.service
sudo adduser --system --group --no-create-home redis
sudo mkdir /var/lib/redis
sudo chown redis:redis /var/lib/redis
sudo chmod 770 /var/lib/redis
sudo systemctl start redis
```
See status using `sudo systemctl status redis


### Deployment
[See here](https://peteris.rocks/blog/deploy-flask-apps-using-anaconda-on-ubuntu-server/) for more detailed explanations.

```
# Build tools (make, gcc, etc.)
sudo apt-get install build-essential -y
source activate flask-api
# make sure following package is installed:
pip install uwsgi
```

Test whether uwsgi is working:
`uwsgi --ini uwsgi.ini`

Write system daemon script

`cd /etc/systemd/system`
Create the following file: `touch /etc/systemd/system/flask.service`:
```
[Unit]
Description=uWSGI instance to serve crowdbreaks-flask-api

[Service]
WorkingDirectory=/home/ubuntu/crowdbreaks-flask-api
ExecStart=/bin/bash -c 'source /home/ubuntu/anaconda3/bin/activate flask-api && /home/ubuntu/anaconda3/bin/uwsgi --ini /home/ubuntu/crowdbreaks-flask-api/uwsgi.ini'

[Install]
WantedBy=multi-user.target
```
Start using:

`sudo systemctl start flask.service`

Make sure server is running properly:

`sudo systemctl status flask.service` or alternatively: `service flask status`

Reverse proxy:

`cd /etc/nginx/sites-available/` 
Create the following file: `touch /etc/nginx/sites-available/logstash-dev.crowdbreaks.org`:
```
server { 
 listen 80;
 server_name logstash-dev.crowdbreaks.org www.logstash-dev.crowdbreaks.org;

 location / {
   proxy_pass http://localhost:8080;
 }
}
```
Make sure file is symlinked to `/etc/nginx/sites-enabled/`, otherwise do:

`sudo ln -s /etc/nginx/sites-available/logstash-dev.crowdbreaks.org  /etc/nginx/sites-enabled/logstash-dev.crowdbreaks.org`

Restart nginx to pick up the changes: 

`sudo systemctl restart nginx`

# Vaccine sentiment tracking

## Trained classifiers
Description of models in `bin/vaccine_sentiment/` 


| Filename | Model type | Description |
| ------ | ------ | ------ |
| `sent2vec_v1.0.p` | sklearn SVM RBF classifier | Trained on data from Bauch et al. (unpublished). Word vectors obtained using sent2vec (based on fastText) and pre-trained model [sent2vec_twitter_bigrams](https://drive.google.com/open?id=0B6VhzidiLvjSeHI4cmdQdXpTRHc) 23GB (700dim, trained on english tweets). |


