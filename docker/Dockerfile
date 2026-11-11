# DozenDB's image: Neo4j's official Community image, with DozenDB's jar in its lib/. The release
# workflow builds it for the Neo4j version the jar runs on, from the jar CI tested:
#
#   docker build -f docker/Dockerfile --build-arg NEO4J_VERSION=5.26.27 \
#     --build-arg DOZENDB_JAR=dozendb-0.2.0-neo4j-5.26.27.jar <directory with the jar>
ARG NEO4J_VERSION
FROM neo4j:${NEO4J_VERSION}-community
ARG DOZENDB_JAR
COPY ${DOZENDB_JAR} /var/lib/neo4j/lib/
