# parity-gateway's image (gateway step 5b; until now parity-deploy carried a copy in
# compose/images/parity-gateway/). Like TC's, the build context is staged: this Dockerfile and the
# jar `mvn package` built, as app.jar (parity-deploy scripts/build-local-images.sh stages it).
# The JRE: Temurin 21 on Ubuntu noble, pinned by digest (the one TC's image pins; 14-day rule).
FROM eclipse-temurin:21.0.12_8-jre-noble@sha256:7739f0ffce786528961eea6bf46d9610ee968ac6127c9b2e93494757bdecce9f
COPY app.jar /app/app.jar
# Non-root, its own UID (compose runs it as the developer's UID to read the 600 secret files).
USER 10015:10015
# The agents' WebSocket and page; the management port (health, metrics). Ports are flags.
EXPOSE 8090 8091
# No secret in the image: the JWKS, the SIP key, the TURN secret and the dev SIP passwords are
# files mounted read-only at run time. Generational ZGC (TC's choice for latency).
ENTRYPOINT ["java", "-XX:+UseZGC", "-XX:+ZGenerational", "-jar", "/app/app.jar"]
