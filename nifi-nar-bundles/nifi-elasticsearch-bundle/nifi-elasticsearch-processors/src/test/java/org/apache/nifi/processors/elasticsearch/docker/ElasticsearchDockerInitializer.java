package org.apache.nifi.processors.elasticsearch.docker;

import java.io.*;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;


public class ElasticsearchDockerInitializer {
    protected static String resourcesFolderPath = "src/test/resources";
    protected static String absolutePath = new File(resourcesFolderPath).getAbsolutePath();
    protected static String osName = System.getProperty("os.name");
    protected static String dockerStartScriptName = absolutePath+"/start-docker."+getScriptExtension(osName);
    protected static String dockerStopScriptName = absolutePath+"/stop-docker."+getScriptExtension(osName);
    protected static String dockerElasticsearchPort = Integer.toString(9200);
    protected static Logger logger = Logger.getLogger(ElasticsearchDockerInitializer.class.getName());


    public static HashMap<DockerContainerType, Integer> writeDockerComposeWithFreePorts() throws IOException {
        HashMap<DockerContainerType, Integer> servicesPorts = new HashMap<>();
        DockerContainerType[] services = {DockerContainerType.SQUID,DockerContainerType.SQUID_AUTH, DockerContainerType.ES01,
                DockerContainerType.ES01_TCP, DockerContainerType.ES02, DockerContainerType.ES02_TCP
        };
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.OWNER_EXECUTE);
        for (DockerContainerType service : services) {
            ServerSocket serverSocket = new ServerSocket(0);
            servicesPorts.put(service, serverSocket.getLocalPort());
        }


        String dockerStartShellScript ="docker network create --subnet=172.18.0.0/16 --gateway=172.18.0.1 es_squid\n" +
                "docker run -e ES_JAVA_OPTS=\"-Xms256m -Xmx256m\" -d " +
                "-p "+servicesPorts.get(DockerContainerType.ES01)+":9200 " +
                "-v "+absolutePath+"/es1.yml:/usr/share/elasticsearch/config/elasticsearch.yml  " +
                "-v data01:/usr/share/elasticsearch/data --network=es_squid --ip 172.18.0.2 --name ES01 elasticsearch:7.4.2\n" +
                "docker run -e ES_JAVA_OPTS=\"-Xms256m -Xmx256m\" -d " +
                "-p "+servicesPorts.get(DockerContainerType.ES02)+":9200 " +
                "-v "+absolutePath+"/es2.yml:/usr/share/elasticsearch/config/elasticsearch.yml  " +
                "-v data02:/usr/share/elasticsearch/data --network=es_squid --ip 172.18.0.3 --name ES02 elasticsearch:7.4.2\n" +
                "docker run --name SQUID -d --restart=always " +
                "--publish "+servicesPorts.get(DockerContainerType.SQUID)+":3228 " +
                "--volume "+absolutePath+"/squid.conf:/etc/squid/squid.conf  " +
                "--volume /srv/docker/squid/cache:/var/spool/squid --network=es_squid sameersbn/squid:3.5.27-2\n" +
                "docker run --name SQUID_AUTH -d --restart=always " +
                "--publish "+servicesPorts.get(DockerContainerType.SQUID_AUTH)+":3328 " +
                "--volume "+absolutePath+"/squid-auth.conf:/etc/squid/squid.conf  " +
                "--volume /srv/docker/squid/cache:/var/spool/squid --network=es_squid sameersbn/squid:3.5.27-2";
        writeFile(dockerStartScriptName,dockerStartShellScript);
        Path dockerStartScriptPath = Paths.get(dockerStartScriptName);
        Files.setPosixFilePermissions(dockerStartScriptPath, perms);
        String dockerStopShellScript =
                "docker stop ES01\n" +
                "docker stop ES02\n" +
                "docker stop SQUID\n" +
                "docker stop SQUID_AUTH\n" +
                "docker rm ES01\n" +
                "docker rm ES02\n" +
                "docker rm SQUID\n" +
                "docker rm SQUID_AUTH\n" +
                "docker network rm es_squid";
        writeFile(dockerStopScriptName, dockerStopShellScript);
        Path dockerStopScriptPath = Paths.get(dockerStopScriptName);
        Files.setPosixFilePermissions(dockerStopScriptPath, perms);
        return servicesPorts;
    }

    public static String getScriptExtension(String osName){
        if(osName.toLowerCase().contains("windows")){
            return "bat";
        }
        return "sh";
    }

    public static void writeFile(String fileName, String script) throws IOException {
        File file = new File(fileName);
        FileWriter fileWriter = new FileWriter(file);
        PrintWriter printWriter = new PrintWriter(fileWriter);
        printWriter.print(script);
        printWriter.close();
    }


    public static void execScript(String scriptName) throws IOException, InterruptedException {
        Process p = Runtime.getRuntime().exec("nohup sh " + scriptName + " &");
        p.waitFor();

        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader errorReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));


        String line;
        while ((line = reader.readLine()) != null) {
            logger.info(line);
        }


        while ((line = errorReader.readLine()) != null) {
            logger.info(line);
        }

        if(scriptName.equals(dockerStartScriptName)) {
            logger.info("Waiting for docker containers to start ...");
            Thread.sleep(15000);
        }
    }
}
