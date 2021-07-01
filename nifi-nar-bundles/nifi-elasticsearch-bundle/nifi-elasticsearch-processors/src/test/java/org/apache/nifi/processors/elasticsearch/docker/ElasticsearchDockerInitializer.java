package org.apache.nifi.processors.elasticsearch.docker;

import org.apache.commons.net.util.SubnetUtils;

import java.io.*;
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
    protected static String elasticsearchSquidDockerStartScriptPathString = resourcesFolderAbsolutePath +"/start-docker.sh";
    protected static String elasticsearchSquidDockerClearScriptPathString = resourcesFolderAbsolutePath +"/clear-docker.sh";
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

    protected static void setScriptPermissions(String scriptPathString) throws IOException {
        Path scriptPath = Paths.get(scriptPathString);
        Files.setPosixFilePermissions(scriptPath, perms);

    }

    protected static void execElasticsearchSquidClearScript () throws IOException, InterruptedException {
        String clearDockerCommand = "nohup sh " + elasticsearchSquidDockerClearScriptPathString + " &";
        Process clearDocker = Runtime.getRuntime().exec(clearDockerCommand);
        clearDocker.waitFor();
        writeCommonLog(clearDocker, clearDockerCommand);
    }

    protected static void execElasticsearchSquidStartScript (HashMap<DockerServicePortType, String> elasticsearchSquidDockerServicesPorts , HashMap<ElasticsearchNodesType, String> elasticsearchServerHosts, String network) throws IOException, InterruptedException {
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
        Process execScript = Runtime.getRuntime().exec(execScriptCommand);
        execScript.waitFor();
        LogWriterComponents startElasticsearchSquidContainers = writeCommonLog(execScript, execScriptCommand);
        String startElasticsearchSquidContainersErrorLogs = startElasticsearchSquidContainers.getErrorLog();
        if(!startElasticsearchSquidContainersErrorLogs.equals("")){
            throw new IOException(startElasticsearchSquidContainersErrorLogs);
        }
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
        execElasticsearchSquidStartScript(elasticsearchSquidDockerServicesPorts, elasticsearchServerHosts, network);
        String curlElasticsearchCommand = "curl http://localhost:" + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.ES01_SP);
        String curlFromSquidToElasticsearchCommand = "curl -x http://localhost:"+ elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_SP) + " " + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_01_IP_ADDRESS) + ":9200";
        String curlFromSquidAuthToElasticsearchCommand = "curl -x http://localhost:"+ elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_AUTH_SP) + " " + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_02_IP_ADDRESS) + ":9200";
        boolean notConnected = true;
        boolean keepWaitingConnection = true;
        Integer attemptsToConnect = 1;
        String errorCurl = "";
        while(keepWaitingConnection) {
            Thread.sleep(10000);
            logger.info("Docker containers initialization. Attempt # " + attemptsToConnect);
            LogWriterComponents logCurlElasticsearch = writeCommonLog(runShellCommand(curlElasticsearchCommand), curlElasticsearchCommand);
            LogWriterComponents logCurlFromSquidToElasticsearch = writeCommonLog(runShellCommand(curlFromSquidToElasticsearchCommand), curlFromSquidToElasticsearchCommand);
            LogWriterComponents logCurlFromSquidAuthToElasticsearch = writeCommonLog(runShellCommand(curlFromSquidAuthToElasticsearchCommand), curlFromSquidAuthToElasticsearchCommand);
            attemptsToConnect = attemptsToConnect + 1;
            String connectionSuccessful = "You Know, for Search";
            if (logCurlElasticsearch.getLog().contains(connectionSuccessful)
                    && logCurlFromSquidToElasticsearch.getLog().contains(connectionSuccessful)
                    && logCurlFromSquidAuthToElasticsearch.getLog().contains(connectionSuccessful))
            {
                notConnected = false;
                keepWaitingConnection = false;
                logger.info("Elasticsearch docker cluster and squid docker containers have started successfully");
            }
            String errorLogCurlElasticsearch = logCurlElasticsearch.getErrorLog();
            String errorLogCurlFromSquidToElasticsearch = logCurlFromSquidToElasticsearch.getErrorLog();
            String errorLogCurlFromSquidAuthToElasticsearch = logCurlFromSquidAuthToElasticsearch.getErrorLog();
            errorCurl = addLogIfNotContained(errorCurl, errorLogCurlElasticsearch, curlElasticsearchCommand);
            errorCurl = addLogIfNotContained(errorCurl,errorLogCurlFromSquidToElasticsearch, curlFromSquidToElasticsearchCommand);
            errorCurl = addLogIfNotContained(errorCurl,errorLogCurlFromSquidAuthToElasticsearch, curlFromSquidAuthToElasticsearchCommand);

            if (attemptsToConnect > 20) {
                keepWaitingConnection = false;
            }
        }
        logger.info("Total amount of connection attempts - " + (attemptsToConnect-1) + "\n" + "Containers started successfully - " + !notConnected);
        if (notConnected) {
            String errorLog =
                    "Docker network used in elasticsearch & squid containers initialization - " + network + "\n" +
                    "The following ip addresses were set for elasticsearch nodes - "
                    + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_01_IP_ADDRESS) + ":9200; "
                    + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_02_IP_ADDRESS) + ":9200"
                    + "\nThe following ip address was set for squid - " +  "localhost:" + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_SP)
                    + "\nThe following ip address was set for squid with authentication parameters - " + "localhost:" + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_AUTH_SP)
                    + "\nTotal amount of connection attempts - " + (attemptsToConnect-1) + ". Containers initialization failed"
                    + errorCurl
                    + "Tip: check if sysctl vm.max_map_count > 262144";
            throw new IOException("Connection not successful. The following errors  emerged while starting the containers: \n"
                   + errorLog
            );
        }
    }

    protected static Process runShellCommand(String shellCommand) throws IOException, InterruptedException {
        Runtime run = Runtime.getRuntime();
        Process pr = run.exec(shellCommand);
        pr.waitFor();
        return pr;
    }

    protected static String addLogIfNotContained(String commonLog, String logLine, String command) {
        if(!commonLog.toLowerCase().contains(logLine.toLowerCase())){
            commonLog = commonLog + "\n" + "Running " + command  + "\n" + logLine + "\n";
        }
        return commonLog;
    }


    protected static PreStartNetworkStatus initializeNetwork(String proxyNetwork) throws IOException, InterruptedException {
        boolean networkExistedBefore = false;
        String startNetworkCommand = "docker network create --subnet=172.18.0.0/16 --gateway=172.18.0.1 " + proxyNetwork;
        LogWriterComponents networkInitializerLogComponents = writeCommonLog(runShellCommand(startNetworkCommand), startNetworkCommand);
        String startNetworkCommandLog = networkInitializerLogComponents.getLog();
        if(startNetworkCommandLog.contains("Pool overlaps with other one on this address space")) {
            logger.info("Trying to find the network set on 172.18.0.0/16 subnet ...");
            networkExistedBefore = true;
            String dockerNetworkListCommand = "docker network ls --format {{.Name}}";
            LogWriterComponents dockerNetworkListLog = writeCommonLog(runShellCommand(dockerNetworkListCommand), dockerNetworkListCommand);
            String dockerNetworkList = dockerNetworkListLog.getLog();
            String[] networkList = dockerNetworkList.split("\n");
            for (String network : networkList) {
                String getNetworkNameWithSubnet = "docker network inspect " + network;
                String getNetworkNameWithSubnetLog = writeCommonLog(runShellCommand(getNetworkNameWithSubnet), getNetworkNameWithSubnet).getLog();
                if (getNetworkNameWithSubnetLog.contains("172.18.0.0")) {
                    proxyNetwork = network;
                    logger.info("Pool with 172.18.0.0 subnet found. It is " + proxyNetwork + ". All docker services will be set on unused hosts of this network.");
                    break;
                }
            }
        }
        if (startNetworkCommandLog.contains("network with name " + proxyNetwork + " already exists")) {
            String getNetworkSubnetCommand = "docker network inspect " + proxyNetwork;
            LogWriterComponents networkInspect = writeCommonLog(runShellCommand(getNetworkSubnetCommand), getNetworkSubnetCommand);
            String networkInspectLog = networkInspect.getLog();
            if(!networkInspectLog.contains("172.18.0.0")){
                throw new IOException("Network with name " + proxyNetwork + " exists, but it is set on different than 172.18.0.0 subnet");
            }
            networkExistedBefore = true;
        }
        return new PreStartNetworkStatus(proxyNetwork,networkExistedBefore);
    }


    protected  static void clearElasticsearchSquidDocker() throws IOException, InterruptedException {
        setScriptPermissions(elasticsearchSquidDockerClearScriptPathString);
        execElasticsearchSquidClearScript();

    }

    protected static LogWriterComponents writeCommonLog(Process process, String command) throws IOException {
        InputStream istream = process.getInputStream();
        InputStream errorIStream = process.getErrorStream();
        Writer writer = new StringWriter();
        Writer errorWriter = new StringWriter();
        char[] buffer = new char[1024];
        Reader reader = new BufferedReader(new InputStreamReader( istream ));
        Reader errorReader = new BufferedReader(new InputStreamReader( errorIStream ));

        int n;
        while ((n = reader.read(buffer)) != -1) {
            writer.write(buffer, 0, n);
        }
        while ((n = errorReader.read(buffer)) != -1) {
            writer.write(buffer, 0, n);
            errorWriter.write(buffer, 0, n);
        }
        logger.info( "Running command - \n" + command + "\n" + writer);
        if(errorWriter.toString().toLowerCase().contains("permission denied")){
            throw new IOException("Docker user does not have permissions to run start container commands");
        }
        LogWriterComponents logWriterComponents = new LogWriterComponents(writer.toString(), errorWriter.toString());
        reader.close();
        errorReader.close();
        istream.close();
        errorIStream.close();
        return logWriterComponents;
    }

}
