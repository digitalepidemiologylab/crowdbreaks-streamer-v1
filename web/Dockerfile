FROM python:3
MAINTAINER Martin Müller "martin.muller@epfl.ch"

# Install python dependencies
COPY ./requirements.txt /home/app/requirements.txt
WORKDIR /home/app
RUN python3 -m pip install --upgrade pip
RUN pip3 install -r requirements.txt

# Install fastText
RUN apt-get update && apt-get install git
RUN git clone https://github.com/facebookresearch/fastText
RUN pip3 install ./fastText/

# Copy files
COPY . /home/app
VOLUME ["/home/app"]
EXPOSE 8000

# Create celery user
RUN adduser --disabled-password --gecos '' celery-user

# development server:
CMD ["python3", "run.py"]
