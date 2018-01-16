FROM docker.elastic.co/logstash/logstash-oss:6.1.1
MAINTAINER Martin Müller "martin.mathias.mueller@gmail.com"

RUN bin/logstash-plugin install logstash-filter-uuid
RUN rm -f /usr/share/logstash/pipeline/logstash.conf

