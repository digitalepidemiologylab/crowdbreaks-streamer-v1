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



# Vaccine sentiment tracking

## Trained classifiers
Description of models in `bin/vaccine_sentiment/` 
| Filename | Model type | Description |
| ------ | ------ | ------ |
| `sent2vec_v1.0.p` | sklearn SVM RBF classifier | Trained on data from Bauch et al. (unpublished). Word vectors obtained using sent2vec (based on fastText) and pre-trained model [sent2vec_twitter_bigrams](https://drive.google.com/open?id=0B6VhzidiLvjSeHI4cmdQdXpTRHc) 23GB (700dim, trained on english tweets). |


