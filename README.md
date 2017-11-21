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
default keys: `config.py`
secret keys: `instance/config.conf`
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
bash Anaconda3-5.0.1-Linux-x86_64.sh
```
Pull repo
```
cd && git clone git clone https://github.com/salathegroup/crowdbreaks-flask-api.git && cd crowdbreaks-flask-api/
git submodule update --init --recursive
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
Install Redis

```
sudo apt-get install make gcc redis-server
cd ~/downloads
wget http://download.redis.io/redis-stable.tar.gz
tar xvzf redis-stable.tar.gz 
cd redis-stable/deps
make hiredis jemalloc linenoise lua geohash-int
cd ..
make
sudo make install
```
Configure and run Redis
```
sudo mkdir /etc/redis
sudo mkdir /var/redis
sudo cp utils/redis_init_script /etc/init.d/redis_6379
sudo cp ~/crowdbreaks-flask-api/other_configs/6379.conf /etc/redis/
sudo mkdir /var/redis/6379
sudo update-rc.d redis_6379 defaults
# Start daemon
sudo /etc/init.d/redis_6379 start
```
See status using `service redis_6379 status`




# Vaccine sentiment tracking

## Trained classifiers
Description of models in `bin/vaccine_sentiment/` 


| Filename | Model type | Description |
| ------ | ------ | ------ |
| `sent2vec_v1.0.p` | sklearn SVM RBF classifier | Trained on data from Bauch et al. (unpublished). Word vectors obtained using sent2vec (based on fastText) and pre-trained model [sent2vec_twitter_bigrams](https://drive.google.com/open?id=0B6VhzidiLvjSeHI4cmdQdXpTRHc) 23GB (700dim, trained on english tweets). |


