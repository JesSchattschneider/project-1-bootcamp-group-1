FROM python:3.9

WORKDIR /app 

COPY /app .

COPY requirements.txt .

RUN pip install -r requirements.txt 

ENV API_KEY=<api_key>
ENV SERVER_NAME=<server_name>
ENV DATABASE_NAME=weather
ENV DB_USERNAME=postgres
ENV DB_PASSWORD=<password>
ENV PORT=5432
ENV LOGGING_SERVER_NAME=<server_name>
ENV LOGGING_DATABASE_NAME=weather
ENV LOGGING_USERNAME=postgres
ENV LOGGING_PASSWORD=<password>
ENV LOGGING_PORT=5432

CMD ["python", "-m", "etl_project.pipelines.weather"]
