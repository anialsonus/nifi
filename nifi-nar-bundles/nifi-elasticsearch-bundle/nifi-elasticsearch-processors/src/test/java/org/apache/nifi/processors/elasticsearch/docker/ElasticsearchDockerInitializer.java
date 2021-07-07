package org.apache.nifi.processors.elasticsearch.docker;

import org.apache.commons.net.util.SubnetUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.logging.Logger;


public class ElasticsearchDockerInitializer {
    protected static String resourcesFolderPath = "src/test/resources";
    protected static String resourcesFolderAbsolutePath = new File(resourcesFolderPath).getAbsolutePath();
    protected static String elasticsearchSquidDockerStartScriptPathString = resourcesFolderAbsolutePath +"/start-docker.sh";
    protected static String elasticsearchSquidDockerClearScriptPathString = resourcesFolderAbsolutePath +"/clear-docker.sh";
    protected static Logger logger = Logger.getLogger(ElasticsearchDockerInitializer.class.getName());


    protected static EnumMap<DockerServicePortType, String> getElasticsearchSquidFreePorts() throws IOException {
        EnumMap<DockerServicePortType, String> servicesPorts = new EnumMap<>(DockerServicePortType.class);
        DockerServicePortType[] elasticsearchSquidDockerServices = {DockerServicePortType.ES_NIFI_01_HTTP_PORT,  DockerServicePortType.ES_NIFI_02_HTTP_PORT,
                DockerServicePortType.ES_NIFI_01_TCP_PORT,  DockerServicePortType.ES_NIFI_02_TCP_PORT,
                DockerServicePortType.SQUID_SP, DockerServicePortType.SQUID_AUTH_SP,
                };
        for (DockerServicePortType service : elasticsearchSquidDockerServices) {
            ServerSocket serverSocket = new ServerSocket(0);
            servicesPorts.put(service, Integer.toString(serverSocket.getLocalPort()));
            serverSocket.close();
        }
        return servicesPorts;
    }


    protected static void execElasticsearchSquidClearScript () throws Exception {
        String clearDockerCommand = "nohup sh " + elasticsearchSquidDockerClearScriptPathString + " &";
        runShellCommandWithLogs(clearDockerCommand);
    }

    protected static void execElasticsearchSquidStartScript (EnumMap<DockerServicePortType, String> elasticsearchSquidDockerServicesPorts , EnumMap<ElasticsearchNodesType, String> elasticsearchServerHosts, String network) throws Exception {
        String execScriptCommand = "nohup sh" +
                " " + elasticsearchSquidDockerStartScriptPathString +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.ES_NIFI_01_HTTP_PORT) +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.ES_NIFI_02_HTTP_PORT) +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_SP) +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_AUTH_SP) +
                " " + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_01_IP_ADDRESS) +
                " " + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_02_IP_ADDRESS) +
                " " + resourcesFolderAbsolutePath +
                " " + network +
                " " + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_01_IP_ADDRESS) +"," + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_02_IP_ADDRESS) +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.ES_NIFI_01_TCP_PORT) +
                " " + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.ES_NIFI_02_TCP_PORT) +
                " &";
        CommandLogComponents startElasticsearchSquidContainers = runShellCommandWithLogs(execScriptCommand);
        String startElasticsearchSquidContainersErrorLogs = startElasticsearchSquidContainers.getErrorLog();
        if(!startElasticsearchSquidContainersErrorLogs.equals("")){
            throw new Exception(startElasticsearchSquidContainersErrorLogs);
        }
    }

    protected static EnumMap<ElasticsearchNodesType, String> getFreeHostsOnSubnet(String subnet) throws Exception {
        SubnetUtils utils = new SubnetUtils(subnet);
        String[] allIpsInSubnet = utils.getInfo().getAllAddresses();
        EnumMap<ElasticsearchNodesType, String> elasticsearchServerHosts = new EnumMap<>(ElasticsearchNodesType.class);
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

    protected static void startElasticsearchSquidDocker(EnumMap<DockerServicePortType, String> elasticsearchSquidDockerServicesPorts, EnumMap<ElasticsearchNodesType, String> elasticsearchServerHosts, String dockerNetworkName) throws Exception {
        execElasticsearchSquidStartScript(elasticsearchSquidDockerServicesPorts, elasticsearchServerHosts, dockerNetworkName);
        String curlElasticsearchCommand = "curl localhost:" + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.ES_NIFI_01_HTTP_PORT);
        String curlFromSquidToElasticsearchCommand = "curl -x localhost:"+ elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_SP) + " " + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_01_IP_ADDRESS) + ":9200";
        String curlFromSquidAuthToElasticsearchCommand = "curl -x localhost:"+ elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_AUTH_SP) + " " + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_02_IP_ADDRESS) + ":9200";
        boolean notConnected = true;
        boolean keepWaitingConnection = true;
        Integer attemptsToConnect = 1;
        String errorCurl = "";
        while(keepWaitingConnection) {
            Thread.sleep(10000);
            logger.info("Docker containers initialization. Attempt # " + attemptsToConnect);
            CommandLogComponents logCurlElasticsearch = runShellCommandWithLogs(curlElasticsearchCommand);
            CommandLogComponents logCurlFromSquidToElasticsearch = runShellCommandWithLogs(curlFromSquidToElasticsearchCommand);
            CommandLogComponents logCurlFromSquidAuthToElasticsearch = runShellCommandWithLogs(curlFromSquidAuthToElasticsearchCommand);
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
            errorCurl = addLogIfNotContained(errorCurl, logCurlElasticsearch.getErrorLog(), curlElasticsearchCommand);
            errorCurl = addLogIfNotContained(errorCurl,logCurlFromSquidToElasticsearch.getErrorLog(), curlFromSquidToElasticsearchCommand);
            errorCurl = addLogIfNotContained(errorCurl,logCurlFromSquidAuthToElasticsearch.getErrorLog(), curlFromSquidAuthToElasticsearchCommand);

            if (attemptsToConnect > 20) {
                keepWaitingConnection = false;
            }
        }
        logger.info("Total amount of connection attempts - " + (attemptsToConnect-1) + "\n" + "Containers started successfully - " + !notConnected);
        if (notConnected) {
            String errorLog =
                    "Docker network used in elasticsearch & squid containers initialization - " + dockerNetworkName + "\n"
                    + "The following ip addresses were set for elasticsearch nodes - "
                    + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_01_IP_ADDRESS) + ":9200; "
                    + elasticsearchServerHosts.get(ElasticsearchNodesType.ES_NODE_02_IP_ADDRESS) + ":9200"
                    + "\nThe following ip address was set for squid - " +  "localhost:" + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_SP)
                    + "\nThe following ip address was set for squid with authentication parameters - " + "localhost:" + elasticsearchSquidDockerServicesPorts.get(DockerServicePortType.SQUID_AUTH_SP)
                    + "\nTotal amount of connection attempts - " + (attemptsToConnect-1) + ". Containers initialization failed"
                    + errorCurl
                    + "\nTip: check if sysctl vm.max_map_count >= 262144";
            throw new Exception("Connection not successful. The following errors  emerged while starting the containers: \n"
                   + errorLog
            );
        }
    }


    protected static String addLogIfNotContained(String commonLog, String logLine, String command) {
        if(!commonLog.toLowerCase().contains(logLine.toLowerCase())){
            commonLog = commonLog + "\n" + "Running " + command  + "\n" + logLine;
        }
        return commonLog;
    }


    protected static PreStartDockerNetworkParams initializeDockerNetwork() throws Exception {
        boolean dockerNetworkExistedBefore = false;
        String dockerNetworkName = "elasticsearch_squid_nifi";
        String dockerNetworkSubnet = "172.18.0.0/16";
        String startNetworkCommand = "docker network create --subnet=" + dockerNetworkSubnet + " --gateway=172.18.0.1 " + dockerNetworkName;
        CommandLogComponents networkInitializerLogComponents = runShellCommandWithLogs(startNetworkCommand);
        String startNetworkCommandLog = networkInitializerLogComponents.getLog();
        if(startNetworkCommandLog.contains("Pool overlaps with other one on this address space")) {
            logger.info("Trying to find the network with pool set on this address space ...");
            dockerNetworkExistedBefore = true;
            String dockerNetworkListCommand = "docker network ls --format {{.Name}}";
            CommandLogComponents dockerNetworkListLog = runShellCommandWithLogs(dockerNetworkListCommand);
            String dockerNetworks = dockerNetworkListLog.getLog();
            String[] dockerNetworkList = dockerNetworks.split("\n");
            for (String network : dockerNetworkList) {
                String networkNameWithSubnetCommand = "docker network inspect " + network;
                String networkNameWithSubnetLog = runShellCommandWithLogs(networkNameWithSubnetCommand).getLog();
                if (networkNameWithSubnetLog.contains("172.18.0.0")) {
                    dockerNetworkName = network;
                    dockerNetworkSubnet = getDockerNetworkSubnet(networkNameWithSubnetLog);
                    logger.info("Network with pool which has overlapped with the necessary address space is found. It is " + dockerNetworkName + ". All docker services will be set on non-functioning ip addresses of this pool.");
                    break;
                }
            }
        }
        if (startNetworkCommandLog.contains("network with name " + dockerNetworkName + " already exists")) {
            String getNetworkSubnetCommand = "docker network inspect " + dockerNetworkName;
            CommandLogComponents networkInspect = runShellCommandWithLogs(getNetworkSubnetCommand);
            String networkInspectLog = networkInspect.getLog();
            dockerNetworkSubnet = getDockerNetworkSubnet(networkInspectLog);
            if(!networkInspectLog.contains("172.18.0.0")){
                throw new Exception("Network with name " + dockerNetworkName + " exists, but it is set on different subnet");
            }
            dockerNetworkExistedBefore = true;
        }
        return new PreStartDockerNetworkParams(dockerNetworkName,dockerNetworkExistedBefore, dockerNetworkSubnet);
    }


    protected  static void clearElasticsearchSquidDocker() throws Exception {
        execElasticsearchSquidClearScript();
    }

    protected static CommandLogComponents runShellCommandWithLogs(String command) throws Exception {
        Runtime run = Runtime.getRuntime();
        Process process = run.exec(command);
        process.waitFor();
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
            throw new Exception("User does not have permissions to run the command - "
            + command
            + "\nFull description from the error log - \n"
            + errorWriter
            );
        }
        CommandLogComponents commandLogComponents = new CommandLogComponents(writer.toString(), errorWriter.toString());
        reader.close();
        errorReader.close();
        istream.close();
        errorIStream.close();
        return commandLogComponents;
    }

    protected static String getDockerNetworkSubnet(String dockerNetworkInspectLog) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONArray dockerNetworkInfoJsonArray = (JSONArray)jsonParser.parse(dockerNetworkInspectLog);
        JSONObject dockerNetworkInfoObject = (JSONObject)dockerNetworkInfoJsonArray.get(0);
        JSONObject dockerNetworkIpamObject = (JSONObject)dockerNetworkInfoObject.get("IPAM");
        JSONArray dockerNetworkConfigArray = (JSONArray)dockerNetworkIpamObject.get("Config");
        JSONObject dockerNetworkConfigObject = (JSONObject)dockerNetworkConfigArray.get(0);
        String dockerNetworkSubnet = dockerNetworkConfigObject.get("Subnet").toString();
        return dockerNetworkSubnet;
    }

}
