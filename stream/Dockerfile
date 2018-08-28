FROM python:3
MAINTAINER Martin Müller "martin.muller@epfl.ch"

# Install python dependencies
COPY ./requirements.txt /home/app/requirements.txt
WORKDIR /home/app
RUN pip3 install -r requirements.txt

# Copy files
COPY . /home/app

CMD ["python", "-u", "run.py"]
