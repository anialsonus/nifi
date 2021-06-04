package org.apache.nifi.processors.elasticsearch.docker;

import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;

public class ElasticsearchDockerInitializer {

    public static final DockerComposeContainer elasticsearchDockerComposeContainer;

    static {
        String path = "src/test/resources/docker-compose.yml";
        File file = new File(path);
        elasticsearchDockerComposeContainer = new DockerComposeContainer(file)
                .withExposedService("es01_proxy",9200)
                .withExposedService("es02_proxy",9200)
                .withExposedService("es03_proxy",9200)
                .withExposedService("squid", 3228)
                .withExposedService("squid_auth", 3328);
    }
}

