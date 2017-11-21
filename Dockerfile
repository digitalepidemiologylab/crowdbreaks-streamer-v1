FROM python:3
MAINTAINER Martin Mueller "martin.mathias.mueller@gmail.com"
WORKDIR /app
ADD . /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt
EXPOSE 80
CMD ["python", "app.py"]

