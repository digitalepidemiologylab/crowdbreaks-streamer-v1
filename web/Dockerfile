FROM python:3
MAINTAINER Martin Müller "martin.mathias.mueller@gmail.com"

COPY ./requirements.txt /home/app/requirements.txt
WORKDIR /home/app
RUN pip3 install -r requirements.txt

COPY . /home/app
VOLUME ["/home/app"]
EXPOSE 5000

# Production
# CMD ["gunicorn", "-c", "gunicorn_config.py", "--log-config", "logging.conf", "wsgi:app"]

# development:
CMD ["python", "run.py"]


