FROM python:3.11

WORKDIR /app

RUN pip install --no-cache-dir mysql-connector-python pandas==1.5.3

COPY mysql_database_setup.py ./database/

CMD ["python", "database/mysql_database_setup.py"]