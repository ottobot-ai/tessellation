FROM ubuntu:22.04

# Use the JDK (headless) rather than the JRE so the JDK diagnostic tools —
# jcmd / jmap / jstack — are present in the node image. These let an operator
# pull a live heap histogram (`jcmd <pid> GC.class_histogram`), thread dump
# (`jstack`), or heap dump (`jmap`) from a running/leaking node container for
# post-mortem, which the JRE-only image could not do. `-headless` drops the
# X11/AWT GUI deps so the image stays lean while still shipping the tools; the
# `java` launcher is fully present, so the entrypoint/CMD are unaffected.
RUN apt update && apt install -y openjdk-21-jdk-headless

# Dev tools
RUN apt install -y curl iproute2 iputils-ping jq

RUN mkdir -p /tessellation/jars

COPY docker/jars/keytool.jar /tessellation/jars/keytool.jar
COPY docker/jars/wallet.jar /tessellation/jars/wallet.jar

COPY docker/jars/gl1.jar /tessellation/jars/gl1.jar
COPY docker/jars/gl0.jar /tessellation/jars/gl0.jar

COPY docker/jars/ml0.jar /tessellation/jars/ml0.jar
COPY docker/jars/dl1.jar /tessellation/jars/dl1.jar
COPY docker/jars/cl1.jar /tessellation/jars/cl1.jar

COPY docker/entrypoint.sh /tessellation/entrypoint.sh
COPY docker/internal-health-check.sh /tessellation/internal-health-check.sh
COPY docker/health-check-inner.sh /tessellation/health-check-inner.sh
RUN chmod +x /tessellation/entrypoint.sh
RUN chmod +x /tessellation/internal-health-check.sh
RUN chmod +x /tessellation/health-check-inner.sh
WORKDIR /tessellation

ENV CL_KEYSTORE=/tessellation/key.p12
ENV CL_COLLATERAL=25000000000000
ENV CL_KEYALIAS="alias"
ENV CL_DEFAULT="password"
ENV CL_APP_ENV="mainnet"


ENTRYPOINT ["/tessellation/entrypoint.sh"]
