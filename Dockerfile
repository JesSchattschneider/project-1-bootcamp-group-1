FROM python:3.9-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ /app/src/

CMD ["python", "-m", "src.extract.pipelines.findwork"]