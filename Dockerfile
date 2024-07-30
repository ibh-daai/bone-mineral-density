FROM python:3.10

# environment variables
ENV PREFECT_API_URL="http://prefect-server:4200/api"

WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .
RUN chmod +x start.sh

ENTRYPOINT ["/usr/src/app/start.sh"]