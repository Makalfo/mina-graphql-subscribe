FROM python:3.7-alpine

# Install Packages and Upgrade
RUN apk add --no-cache --update \
    python3 python3-dev gcc g++ \
    gfortran musl-dev \
    libffi-dev openssl-dev libpq-dev
RUN pip install --upgrade pip
RUN apk --update add gcc make cmake g++ zlib-dev

# Working Directory 
RUN mkdir /mina/
WORKDIR /mina/

# Install python requirements
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Set timezone
RUN apk add --no-cache tzdata
ENV TZ America/Denver

# Copy Python Files
COPY CodaClient.py .
COPY Mina_GraphQL.py .

# Run Bot
CMD ["python3", "Mina_GraphQL.py"]