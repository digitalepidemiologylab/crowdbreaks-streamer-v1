# Crowdbreaks Streamer

Intended to be used for stream processing, interaction with ElasticSearch, and endpoints for running ML tasks (such as vaccine sentiments) and exposes Flask API endpoints (on `api.crowdbreaks.org`).

# Installation
## Development
```
git clone git@github.com:crowdbreaks/crowdbreaks-streamer.git && cd crowdbreaks-streamer
cp secrets.list.example secrets.list
```
Then set environment variables in `secrets.list`. Afterwards download model binaries using:
```
source scripts/download_binaries.sh 
```
Make sure docker is running in the background, then run:
```
docker-compose up --build
# or
source build_development.sh
```
Run tests:
```
pytest web/tests      # make sure all dependencies are installed and ENV vars are set
# or
source build_test.sh  # runs everything in docker, no setup necessary
```

For more info read the [wiki](https://github.com/crowdbreaks/crowdbreaks-crowdbreaks-streamer/wiki/Development)

## Production EC2 (Ubuntu 16.04)
After setting env variables, run:
```
source build_production.sh
```
For more info read the [wiki](https://github.com/crowdbreaks/crowdbreaks-streamer/wiki/Deployment)


# Vaccine sentiment tracking

## Trained classifiers
Description of models in `bin/vaccine_sentiment/` 


| Filename | Model type | Description |
| ------ | ------ | ------ |
| `sent2vec_v1.0.p` | sklearn SVM RBF classifier | Trained on data from Bauch et al. Word vectors obtained using sent2vec (based on fastText) and pre-trained model [sent2vec_twitter_bigrams](https://drive.google.com/open?id=0B6VhzidiLvjSeHI4cmdQdXpTRHc) 23GB (700dim, trained on english tweets). |
| `fasttext_v1.ftz` | FastText supervised [bag-of-words classifier](https://arxiv.org/pdf/1607.01759.pdf) | Trained on data from Bauch et al. Parameters: dim=100, epochs=200, ngrams=3, learning_rate=0.015 |


## Example usage
Determine vaccine sentiment of a sentence
```
import requests, os
post_url = 'http://localhost:8000/sentiment/vaccine/'
data = {'text': 'You should get vaccinated'}
resp = requests.post(post_url, json=data, auth=(os.environ.get('BASIC_AUTH_USERNAME'), os.environ.get('BASIC_AUTH_PASSWORD')))
resp.json()
{
   'labels': ['pro-vaccine', 'neutral', 'anti-vaccine'], 
   'probabilities': [0.9998367428779602, 0.00018288915453013033, 1.035925106407376e-05], 
   'model': 'fasttext_v1'
}
```

# Contact
In case of questions feel free to write to info@crowdbreaks.org or directly to Martin Müller (martin.muller@epfl.ch)
