FROM python:3.11

WORKDIR /app

RUN pip install pandas==1.5.3 requests==2.29.0

COPY base_data_extract.py ./src/

COPY ExtractFunctionsDST.py ./src/

COPY Authentication_key_retrieval.py ./src/

ENV LOG 1 

CMD ["python", "./src/base_data_extract.py"]