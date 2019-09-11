FROM ubuntu

# Install packages
RUN apt-get update && \
apt-get install -y curl
COPY ./run.sh /
RUN chmod +x /run.sh
COPY mappings.json /

ENTRYPOINT ["sh", "/run.sh"]
