FROM python:3.8-slim-buster as compile-image


WORKDIR /

RUN apt-get update && apt-get install curl -y && apt-get install -y gnupg2 && apt-get install -y ca-certificates wget && \
    apt-get update && apt upgrade -y && \
# install and update pip
    python3 -m pip install --upgrade pip && \
# Install build tools
    apt-get install apt-transport-https -y  && apt-get install build-essential -y && \
# Install MS ODBC Driver
    apt-get install unixodbc-dev -y  && \
# Set up virtual env
    pip3 install virtualenv && \
    virtualenv -p python3.8 /opt/codificator-etl/venv/


WORKDIR /opt/codificator-etl/

# Copy and install packages
COPY . .
# activate virtualenv and install fwg-codificator-etl and all dependencies
RUN . venv/bin/activate && pip install -r requirements.txt


FROM python:3.8-slim-buster AS runtime-image

# Copy files 
COPY --from=compile-image /opt/codificator-etl /opt/codificator-etl
COPY ./entrypoint.sh /opt/codificator-etl/entrypoint.sh
COPY . /opt/codificator-etl

# Update Ubuntu
RUN apt-get update && \
# update repos to install openjdk java
    apt-get install curl -y && apt-get install -y wget && apt-get install -y ca-certificates  && \
# gnupg2 and unixodbc is required for java installation
    apt-get install gnupg2 -y && apt-get install unixodbc -y && \
    wget -O- - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - && \
    printf "deb https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ buster main" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && apt upgrade -y && \
# Install Java
    apt-get install apt-transport-https -y  && \
    apt install adoptopenjdk-8-hotspot -y && \
# Install MS ODBC Driver and odbc tools
    wget https://packages.microsoft.com/debian/10/prod/pool/main/m/msodbcsql17/msodbcsql17_17.8.1.1-1_amd64.deb && \
    ACCEPT_EULA=Y dpkg -i msodbcsql17_17.8.1.1-1_amd64.deb && \
    rm msodbcsql17_17.8.1.1-1_amd64.deb && \
# Clean system
    apt-get clean && rm -rf /var/lib/apt/lists/*  && \
# Set entry point & execution rights
    chmod +x /opt/codificator-etl/entrypoint.sh


ENTRYPOINT ["/opt/codificator-etl/entrypoint.sh"]
