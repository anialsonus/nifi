package org.apache.nifi.processors.elasticsearch.docker;

public enum DockerServicePortType {
    ES_NIFI_01_HTTP_PORT,
    ES_NIFI_02_HTTP_PORT,
    ES_NIFI_01_TCP_PORT,
    ES_NIFI_02_TCP_PORT,
    SQUID_HTTP_PORT,
    SQUID_AUTH_HTTP_PORT
}
