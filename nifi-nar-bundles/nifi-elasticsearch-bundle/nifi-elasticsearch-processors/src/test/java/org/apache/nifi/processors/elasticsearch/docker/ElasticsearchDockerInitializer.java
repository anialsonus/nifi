package org.apache.nifi.processors.elasticsearch.docker;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.logging.Logger;


 public class ElasticsearchDockerInitializer {
    protected static String resourcesFolderPath = "src/test/resources";
    protected static String resourcesFolderAbsolutePath = new File(resourcesFolderPath).getAbsolutePath();
    protected static String osName = System.getProperty("os.name");
    protected static String elasticsearchSquidDockerStartScriptPathString = resourcesFolderAbsolutePath +"/start-docker."+getScriptExtension(osName);
    protected static String elasticsearchSquidDockerStopScriptPathString = resourcesFolderAbsolutePath +"/stop-docker."+getScriptExtension(osName);
    protected static Logger logger = Logger.getLogger(ElasticsearchDockerInitializer.class.getName());
    protected static Set<PosixFilePermission> perms = new HashSet<>();

    static {
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.OWNER_EXECUTE);
    }


    protected static HashMap<DockerServicePortType, String> getElasticsearchSquidFreePorts() throws IOException {
        HashMap<DockerServicePortType, String> servicesPorts = new HashMap<>();
        DockerServicePortType[] elasticsearchSquidDockerServices = {DockerServicePortType.ES01_SP,  DockerServicePortType.ES02_SP,
                DockerServicePortType.ES_CP, DockerServicePortType.SQUID_SP, DockerServicePortType.SQUID_AUTH_SP,
                };
        for (DockerServicePortType service : elasticsearchSquidDockerServices) {
            ServerSocket serverSocket = new ServerSocket(0);
            servicesPorts.put(service, Integer.toString(serverSocket.getLocalPort()));
            serverSocket.close();
        }
        return servicesPorts;
    }

    protected static String getScriptExtension(String osName){
        if(osName.toLowerCase().contains("windows")){
            return "bat";
        }
        return "sh";
    }
    
    protected static void setScriptPermissions(String scriptPathString) throws IOException {
        Path scriptPath = Paths.get(scriptPathString);
        Files.setPosixFilePermissions(scriptPath, perms);

    }

    protected static Process execElasticsearchSquidStopScript () throws IOException {
        return Runtime.getRuntime().exec("nohup sh " + elasticsearchSquidDockerStopScriptPathString + " &");
    }

    protected static Process execElasticsearchSquidStartScript (HashMap<DockerServicePortType, String> elasticsearchSquidDockerServicesPorts) throws IOException {
        String execScriptCommand = "nohup sh" +
                " " + elasticsearchSquidDockerStartScriptPathString +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.ES01_SP) +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.ES02_SP) +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.ES_CP) +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_SP) +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_AUTH_SP) +
                " " + resourcesFolderAbsolutePath +
                " &";
        return Runtime.getRuntime().exec(execScriptCommand);
    }

    protected static void startElasticsearchSquidDocker(HashMap<DockerServicePortType, String> elasticsearchSquidDockerServicesPorts) throws IOException, InterruptedException {
        editYmlFile(resourcesFolderAbsolutePath + "/es1.yml" , elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.ES_CP));
        editYmlFile(resourcesFolderAbsolutePath + "/es2.yml" , elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.ES_CP));
        setScriptPermissions(elasticsearchSquidDockerStartScriptPathString);
        Process p = execElasticsearchSquidStartScript(elasticsearchSquidDockerServicesPorts);
        p.waitFor();
        showLogsDuringScriptExecution(p);
        logger.info("Waiting for docker containers to start ...");
        Thread.sleep(15000);
    }

    protected  static void stopElasticsearchSquidDocker() throws IOException, InterruptedException {
        setScriptPermissions(elasticsearchSquidDockerStopScriptPathString);
        Process p = execElasticsearchSquidStopScript();
        p.waitFor();
        showLogsDuringScriptExecution(p);
    }

    protected static void showLogsDuringScriptExecution(Process p) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader errorReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            logger.info(line);
        }
        while ((line = errorReader.readLine()) != null) {
            logger.info(line);
        }
    }

    protected static void editYmlFile(String ymlPathString, String port) throws IOException {
        FileInputStream fisTargetFile = new FileInputStream(ymlPathString);
        String ymlContentString = IOUtils.toString(fisTargetFile, "UTF-8");
        LinkedHashMap<String,String> ymlMap = convertStringToLinkedHashMapWithArray(ymlContentString);
        ymlMap.put("http.port", " " + port);
        BufferedWriter  bf = new BufferedWriter(new FileWriter(ymlPathString));
        for (Map.Entry<String, String> entry :
                ymlMap.entrySet()) {
            bf.write(entry.getKey() + ":"
                    + entry.getValue());
            bf.newLine();
        }
        bf.flush();
    }

    protected static LinkedHashMap<String,String> convertStringToLinkedHashMapWithArray(String s) {
        LinkedHashMap<String, String> myMap = new LinkedHashMap<>();
        String[] pairs = s.split("\n");
        for (int i = 0; i < pairs.length; i++) {
            String pair = pairs[i];
            String[] keyValue = pair.split(":");
            myMap.put(keyValue[0], keyValue[1]);
        }
        return myMap;
    }
}
