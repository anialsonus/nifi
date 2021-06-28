package org.apache.nifi.processors.elasticsearch.docker;

import org.apache.commons.net.util.SubnetUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;


 public class ElasticsearchDockerInitializer {
    protected static String resourcesFolderPath = "src/test/resources";
    protected static String resourcesFolderAbsolutePath = new File(resourcesFolderPath).getAbsolutePath();
    protected static String osName = System.getProperty("os.name");
    protected static String elasticsearchSquidDockerStartScriptPathString = resourcesFolderAbsolutePath +"/start-docker."+getScriptExtension(osName);
    protected static String elasticsearchSquidDockerClearScriptPathString = resourcesFolderAbsolutePath +"/clear-docker."+getScriptExtension(osName);
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
                DockerServicePortType.SQUID_SP, DockerServicePortType.SQUID_AUTH_SP,
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

    protected static Process execElasticsearchSquidClearScript () throws IOException {
        return Runtime.getRuntime().exec("nohup sh " + elasticsearchSquidDockerClearScriptPathString + " &");
    }

    protected static Process execElasticsearchSquidStartScript (HashMap<DockerServicePortType, String> elasticsearchSquidDockerServicesPorts , HashMap<ElasticsearchNodesType, String> elasticsearchServerHosts, String network) throws IOException {
        String execScriptCommand = "nohup sh" +
                " " + elasticsearchSquidDockerStartScriptPathString +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.ES01_SP) +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.ES02_SP) +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_SP) +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_AUTH_SP) +
                " " + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_01_IP_ADDRESS) +
                " " + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_02_IP_ADDRESS) +
                " " + resourcesFolderAbsolutePath +
                " " + network +
                " " + "[\"" + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_01_IP_ADDRESS) +"\",\"" + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_02_IP_ADDRESS) + "\"]" +
                " &";
        logger.info(execScriptCommand);
        return Runtime.getRuntime().exec(execScriptCommand);
    }

    protected static HashMap<ElasticsearchNodesType, String> getFreeHostsOnSubnet() throws IOException {
        SubnetUtils utils = new SubnetUtils("172.18.0.0/16");
        String[] allIpsInSubnet = utils.getInfo().getAllAddresses();
        HashMap<ElasticsearchNodesType, String> elasticsearchServerHosts = new HashMap<>();
        ArrayList<String> elasticsearchIps = new ArrayList<>();
        ArrayList<ElasticsearchNodesType> serverNodes = new ArrayList<>();
        serverNodes.add(ElasticsearchNodesType.ES_NODE_01_IP_ADDRESS);
        serverNodes.add(ElasticsearchNodesType.ES_NODE_02_IP_ADDRESS);
        for (String ip: allIpsInSubnet) {
            if(elasticsearchIps.size() >= 2) {
                break;
            }
            if (!InetAddress.getByName(ip).isReachable(5000)) {
                elasticsearchIps.add(ip);
            }
        }
        for(int i = 0; i < serverNodes.size(); i++) {
            elasticsearchServerHosts.put(serverNodes.get(i), elasticsearchIps.get(i));
        }
        return elasticsearchServerHosts;
    }

    protected static void startElasticsearchSquidDocker(HashMap<DockerServicePortType, String> elasticsearchSquidDockerServicesPorts, HashMap<ElasticsearchNodesType, String> elasticsearchServerHosts, String network) throws IOException, InterruptedException {
        setScriptPermissions(elasticsearchSquidDockerStartScriptPathString);
        Process p = execElasticsearchSquidStartScript(elasticsearchSquidDockerServicesPorts, elasticsearchServerHosts, network);
        p.waitFor();
        logger.info("Waiting for docker containers to start ...");
        LogStatistics logElasticsearchSquidDocker = getLogsDuringScriptExecution(p);
        String curlElasticsearch = "curl http://localhost:" + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.ES01_SP);
        String curlFromSquidToElasticsearch = "curl -x http://localhost:"+ elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_SP) + " " + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_01_IP_ADDRESS) + ":9200";
        String curlFromSquidAuthToElasticsearch = "curl -x http://localhost:"+ elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_AUTH_SP) + " " + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_02_IP_ADDRESS) + ":9200";
        logger.info(curlElasticsearch);
        logger.info(curlFromSquidToElasticsearch);
        logger.info(curlFromSquidAuthToElasticsearch);
        boolean notConnected = true;
        boolean keepWaitingConnection = true;
        Integer countSleep = 0;
        while(keepWaitingConnection) {
            Thread.sleep(1000);
            LogStatistics logCurlElasticsearch = getLogsDuringScriptExecution(runShellCommand(curlElasticsearch));
            LogStatistics logCurlFromSquidToElasticsearch = getLogsDuringScriptExecution(runShellCommand(curlFromSquidToElasticsearch));
            LogStatistics logCurlFromSquidAuthToElasticsearch = getLogsDuringScriptExecution(runShellCommand(curlFromSquidAuthToElasticsearch));
            countSleep = countSleep + 1000;
            String connectionSuccessful = "You Know, for Search";
            if (logCurlElasticsearch.getLog().contains(connectionSuccessful)
                    && logCurlFromSquidToElasticsearch.getLog().contains(connectionSuccessful)
                    && logCurlFromSquidAuthToElasticsearch.getLog().contains(connectionSuccessful))
            {
                notConnected = false;
                keepWaitingConnection = false;
                logger.info("Elasticsearch docker cluster and squid docker containers have started successfully");
            }
            if (countSleep >= 60000) {
                keepWaitingConnection = false;
            }
        }
        if (notConnected) {
            throw new IOException("Connection not successful. The following errors were emerged while starting the containers: \n"
                   + logElasticsearchSquidDocker.getErrorLog());
        }
    }

    protected static Process runShellCommand(String shellCommand) throws IOException, InterruptedException {
        Runtime run = Runtime.getRuntime();
        Process pr = run.exec(shellCommand);
        pr.waitFor();
        return pr;
    }


    protected static PreStartNetworkStatus initializeNetwork(String proxyNetwork) throws IOException, InterruptedException {
        boolean networkExistedBefore = false;
        String startNetworkCommand = "docker network create --subnet=172.18.0.0/16 --gateway=172.18.0.1 " + proxyNetwork;
        LogStatistics startNetworkCommandLog = getLogsDuringScriptExecution(runShellCommand(startNetworkCommand));
        if(startNetworkCommandLog.getLog().contains("Pool overlaps with other one on this address space")) {
            networkExistedBefore = true;
            String getDockerNetworkList = "docker network ls --format \"{{.Name}}\"";
            LogStatistics dockerNetworkListLog = getLogsDuringScriptExecution(runShellCommand(getDockerNetworkList));
            String networkListWithoutQuotes = dockerNetworkListLog.getLog().replaceAll("\"", "");
            String[] networkList = networkListWithoutQuotes.split("\n");
            for (String network : networkList) {
                String getNetworkNameWithSubnet = "docker network inspect " + network;
                LogStatistics getNetworkNameWithSubnetLog = getLogsDuringScriptExecution(runShellCommand(getNetworkNameWithSubnet));
                if (getNetworkNameWithSubnetLog.getLog().contains("172.18.0.0")) {
                    proxyNetwork = network;
                    break;
                }
            }
        }
        if (startNetworkCommandLog.getLog().contains("network with name " + proxyNetwork + " already exists")) {
            logger.info("Network with such name already exists");
            networkExistedBefore = true;
        }

        return new PreStartNetworkStatus(proxyNetwork,networkExistedBefore);
    }


    protected  static void clearElasticsearchSquidDocker() throws IOException, InterruptedException {
        setScriptPermissions(elasticsearchSquidDockerClearScriptPathString);
        Process p = execElasticsearchSquidClearScript();
        p.waitFor();
        getLogsDuringScriptExecution(p);
    }

    protected static LogStatistics getLogsDuringScriptExecution(Process p) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader errorReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String line;
        String log = "";
        String errorLog = "";
        while ((line = reader.readLine()) != null) {
            log = log + line + "\n";
        }
        while ((line = errorReader.readLine()) != null) {
            log = log + line + "\n";
            errorLog = errorLog + line +"\n";
        }
        logger.info(log);
        return new LogStatistics(log,errorLog);
    }
}
