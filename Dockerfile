FROM python:3.11.8-slim

WORKDIR /code

ADD requirements.txt .

RUN pip install -r requirements.txt

ADD WebScrapping_Graham.py .

CMD ["python3", "WebScrapping_Graham.py"] 