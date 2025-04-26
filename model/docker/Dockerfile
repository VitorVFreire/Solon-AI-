FROM neo4j:5.17

USER root
RUN apt-get update && \
    apt-get install -y curl vim && \
    apt-get clean

USER neo4j
