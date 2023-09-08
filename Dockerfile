FROM python:3.8
WORKDIR /app
COPY main.py .

RUN pip install requests
RUN pip install pandas
RUN pip install psycopg2
RUN pip install sqlalchemy
RUN pip install redshift_connector
RUN pip install sqlalchemy-redshift

CMD ["python", "main.py"]

