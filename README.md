# Crowdbreaks Flask API 

Intended to be used for stream processing (currently from logstash), interaction with ElasticSearch, and endpoints for running ML tasks (such as vaccine sentiments).

# Usage
Determine vaccine sentiment of text
```
import requests
import instance.config  # secret configs

post_url = '{}/sentiment/vaccine'.format(instance.config.FLASK_API_HOSTNAME)
data = {'text': 'Give me the sentiment of this string'}
resp = requests.post(post_url, json=data, auth=(instance.config.FLASK_API_USERNAME, instance.config.FLASK_API_PASSWORD))
# Response
{  
   "label":"neutral",
   "distances":[  
      -0.3229041893551316,
      2.4999999999999996,
      0.8229041893551319
   ]
}
```
Distances are the distances to the hyperplanes for all pairs of classes -1, 0, 1 (anti-vaccine, neutral, pro-vaccine), [read more](http://scikit-learn.org/stable/modules/generated/sklearn.svm.SVC.html#sklearn.svm.SVC.decision_function)

# Installation
## Development (MacOSX)
```
git clone git@github.com:salathegroup/crowdbreaks-flask-api.git && cd crowdbreaks-flask-api
conda env create -f environment.yml
source activate crowdbreaks-flask

# start server
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

Other services:

```
brew install redis
brew install logstash
```
For more info read the [wiki](https://github.com/salathegroup/crowdbreaks-flask-api/wiki/Development)

## Production EC2 (Ubuntu 16.04)
See [wiki](https://github.com/salathegroup/crowdbreaks-flask-api/wiki/Deployment)

# Vaccine sentiment tracking

## Trained classifiers
Description of models in `bin/vaccine_sentiment/` 


| Filename | Model type | Description |
| ------ | ------ | ------ |
| `sent2vec_v1.0.p` | sklearn SVM RBF classifier | Trained on data from Bauch et al. (unpublished). Word vectors obtained using sent2vec (based on fastText) and pre-trained model [sent2vec_twitter_bigrams](https://drive.google.com/open?id=0B6VhzidiLvjSeHI4cmdQdXpTRHc) 23GB (700dim, trained on english tweets). |


# Author
Martin Müller (martin.muller@epfl.ch)
