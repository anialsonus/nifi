package org.apache.nifi.processors.elasticsearch.docker;

import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;

public abstract class ElasticsearchDockerInitializer {

    public static final DockerComposeContainer elasticsearchDockerComposeContainer;

    static {
        String path = "src/test/resources/docker-compose.yml";
        File file = new File(path);
        elasticsearchDockerComposeContainer = new DockerComposeContainer(file)
                .withExposedService("es01",9200)
                .withExposedService("es02",9200)
                .withExposedService("es03",9200);

    }
}
