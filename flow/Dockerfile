FROM python:3.10

WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .
RUN chmod +x start.sh

ENTRYPOINT ["/usr/src/app/start.sh"]