# syntax=docker/dockerfile:1
FROM python:alpine
RUN pip install --upgrade pip
WORKDIR /app
COPY app/ .
RUN pip3 install -r requirements.txt
CMD [ "python", "-u", "app.py"]
