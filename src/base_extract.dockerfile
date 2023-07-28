FROM python:3.11

WORKDIR /app

RUN pip install pandas

COPY src/base_data_extract.py ./src/

ENV LOG 1 

CMD ["python", "./src/base_data_extract.py"]