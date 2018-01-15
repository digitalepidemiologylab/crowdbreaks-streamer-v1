FROM docker.elastic.co/logstash/logstash-oss:6.1.1
RUN bin/logstash-plugin install logstash-filter-uuid
RUN rm -f /usr/share/logstash/pipeline/logstash.conf

