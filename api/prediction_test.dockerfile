FROM python:3.11

WORKDIR /app

RUN pip install --no-cache-dir requests==2.29.0

COPY test/prediction_test.py ./api/tests

ENV LOG 1 

CMD ["python", "./api/test/prediction_test.py"]