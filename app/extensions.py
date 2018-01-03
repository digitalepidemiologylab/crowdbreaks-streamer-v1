from app.connections import elastic, redis

es = elastic.Elastic()
redis = redis.Redis()
