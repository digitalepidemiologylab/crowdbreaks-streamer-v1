FROM nginx:1.13
MAINTAINER Martin Müller "martin.mathias.mueller@gmail.com"

RUN rm /etc/nginx/conf.d/default.conf
COPY nginx.conf /etc/nginx/nginx.conf.template
EXPOSE 80 443

