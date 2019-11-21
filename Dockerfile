FROM python:alpine

WORKDIR /app

# Install requirements
ADD requirements.txt .
RUN pip install -r requirements.txt

# Add code
ADD . .

# Serve the API
#USER 1000:1000
ENTRYPOINT flask run --host=0.0.0.0
