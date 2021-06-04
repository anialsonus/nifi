package org.apache.nifi.processors.elasticsearch.docker;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;

public class ElasticsearchDockerInitializer {

    public static DockerComposeContainer initiateElasticsearchDockerComposeContainer(DockerComposeContainerType type) {
        File composeFile = initiateFile(type);
        DockerComposeContainer basicElasticsearchDockerComposeContainer = new DockerComposeContainer(composeFile)
                .withExposedService("es01", 9200)
                .withExposedService("es02", 9200)
                .withExposedService("es03", 9200);
        switch(type){
            case BASIC: break;
            case PROXY:
                basicElasticsearchDockerComposeContainer
                .withExposedService("squid", 3228)
                .withExposedService("squid_auth", 3328);
                break;
        }
        return basicElasticsearchDockerComposeContainer;
    }

    private static File initiateFile(DockerComposeContainerType type){
        File composeFile;
        switch(type){
            case BASIC:
                composeFile = new File("src/test/resources/docker-compose.yml");
                break;
            case PROXY:
                composeFile = new File("src/test/resources/proxy-squid/docker-compose-proxy.yml");
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        return composeFile;
    }

}
