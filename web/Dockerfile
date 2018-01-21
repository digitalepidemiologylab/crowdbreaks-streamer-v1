FROM python:3
MAINTAINER Martin Müller"martin.muller@epfl.ch"

COPY ./requirements.txt /home/app/requirements.txt
WORKDIR /home/app
RUN pip3 install -r requirements.txt

# Install fastText
RUN apt-get update && apt-get install git
RUN git clone https://github.com/facebookresearch/fastText
RUN pip3 install ./fastText/

COPY . /home/app
VOLUME ["/home/app"]
EXPOSE 8000
RUN adduser --disabled-password --gecos '' celery-user

# development server:
CMD ["python", "run.py"]


