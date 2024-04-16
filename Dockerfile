FROM maven:3.9-eclipse-temurin-21-alpine AS build

RUN mkdir -p /app
WORKDIR /app
COPY . /app/burgerlinker

RUN if [ ! -f /app/burgerlinker/pom.xml ]; then \
      rm -rf burgerlinker; \
      git clone https://github.com/CLARIAH/burgerLinker.git; \
    fi

RUN cd /app/burgerlinker && \
    mvn install

FROM eclipse-temurin:21-jre-alpine

RUN apk update && apk upgrade && \
    apk add --no-cache python3 py3-pandas

RUN mkdir -p /app
WORKDIR /app

COPY --from=build /app/burgerlinker/target/burgerLinker-0.0.1-SNAPSHOT-jar-with-dependencies.jar /app/
COPY --from=build /app/burgerlinker/assets/docker/entrypoint.sh /app/
COPY --from=build /app/burgerlinker/assets/csv-to-rdf/zeeland-dataset/script.py /app/convert-to-RDF.py

ENTRYPOINT ["/app/entrypoint.sh"]
