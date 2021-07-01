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
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;


public class ElasticsearchDockerInitializer {
    protected static String resourcesFolderPath = "src/test/resources";
    protected static String resourcesFolderAbsolutePath = new File(resourcesFolderPath).getAbsolutePath();
    protected static String osName = System.getProperty("os.name");
    protected static String elasticsearchSquidDockerStartScriptPathString = resourcesFolderAbsolutePath +"/start-docker."+getScriptExtension(osName);
    protected static String elasticsearchSquidDockerClearScriptPathString = resourcesFolderAbsolutePath +"/clear-docker."+getScriptExtension(osName);
    protected static Logger logger = Logger.getLogger(ElasticsearchDockerInitializer.class.getName());
    protected static Set<PosixFilePermission> perms = new HashSet<>();
    protected static PrintStream prStr;
    protected static StreamHandler streamHandler;
    protected static ByteArrayOutputStream loggerContent;
    protected static String log;
    protected static String errorLog;

    static {
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.OWNER_EXECUTE);

        loggerContent = new ByteArrayOutputStream();
        prStr = new PrintStream(loggerContent);
        streamHandler = new StreamHandler(prStr, new SimpleFormatter());
        logger.addHandler(streamHandler);
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

    protected static void execElasticsearchSquidClearScript () throws IOException, InterruptedException {
        String clearDockerCommand = "nohup sh " + elasticsearchSquidDockerClearScriptPathString + " &";
        Process clearDocker = Runtime.getRuntime().exec(clearDockerCommand);
        clearDocker.waitFor();
        closeStreams(writeCommonLog(clearDocker, clearDockerCommand));
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
        addLog("Starting script to initiate docker containers on random free hosts of specified subnet (172.18.0.0/16) with the following command " + "\n"
        + execScriptCommand + "\n");
        Process execScript = Runtime.getRuntime().exec(execScriptCommand);
        execScript.waitFor();
        writeLogs(execScript, execScriptCommand);
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
        log = log + "\n" + curlElasticsearchCommand + "\n" + curlFromSquidToElasticsearchCommand + "\n"  + curlFromSquidAuthToElasticsearchCommand + "\n" ;
        boolean notConnected = true;
        boolean keepWaitingConnection = true;
        Integer attemptsToConnect = 1;
        while(keepWaitingConnection) {
            Thread.sleep(3000);
            addLog("Docker containers initialization. Attempt # " + attemptsToConnect);
            String logCurlElasticsearch = writeLogs(runShellCommand(curlElasticsearchCommand), curlElasticsearchCommand);
            String logCurlFromSquidToElasticsearch = writeLogs(runShellCommand(curlFromSquidToElasticsearchCommand), curlFromSquidToElasticsearchCommand);
            String logCurlFromSquidAuthToElasticsearch = writeLogs(runShellCommand(curlFromSquidAuthToElasticsearchCommand), curlFromSquidAuthToElasticsearchCommand);
            attemptsToConnect = attemptsToConnect + 1;
            String connectionSuccessful = "You Know, for Search";
            if (logCurlElasticsearch.contains(connectionSuccessful)
                    && logCurlFromSquidToElasticsearch.contains(connectionSuccessful)
                    && logCurlFromSquidAuthToElasticsearch.contains(connectionSuccessful))
            {
                notConnected = false;
                keepWaitingConnection = false;
                addLog("Elasticsearch docker cluster and squid docker containers have started successfully");
            }
            if (attemptsToConnect > 20) {
                keepWaitingConnection = false;
            }
        }
        addLog("Total amount of connection attempts - " + (attemptsToConnect-1) + "\n" + "Containers started successfully - " + !notConnected);
        if (notConnected) {
            addErrorLog("Total amount of connection attempts - " + (attemptsToConnect-1) + ". Containers initialization failed");
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


    protected static PreStartNetworkStatus initializeNetwork(String proxyNetwork) throws IOException, InterruptedException {
        boolean networkExistedBefore = false;
        String startNetworkCommand = "docker network create --subnet=172.18.0.0/16 --gateway=172.18.0.1 " + proxyNetwork;
        LogWriterComponents networkInitializerLogComponents = writeCommonLog(runShellCommand(startNetworkCommand), startNetworkCommand);
        String startNetworkCommandLog = networkInitializerLogComponents.getLogWriter().toString();
        if(startNetworkCommandLog.contains("Pool overlaps with other one on this address space")) {
            addLog("Trying to find the network set on 172.18.0.0/16 subnet ...");
            networkExistedBefore = true;
            String getDockerNetworkList = "docker network ls --format \"{{.Name}}\"";
            String dockerNetworkListLog = writeLogs(runShellCommand(getDockerNetworkList), getDockerNetworkList);
            String networkListWithoutQuotes = dockerNetworkListLog.replaceAll("\"", "");
            String[] networkList = networkListWithoutQuotes.split("\n");
            for (String network : networkList) {
                String getNetworkNameWithSubnet = "docker network inspect " + network;
                String getNetworkNameWithSubnetLog = writeLogs(runShellCommand(getNetworkNameWithSubnet), getNetworkNameWithSubnet);
                if (getNetworkNameWithSubnetLog.contains("172.18.0.0")) {
                    proxyNetwork = network;
                    addLog("Pool with 172.18.0.0 subnet found. It is " + proxyNetwork + ". All docker services will be set on unused hosts of this network.");
                    break;
                }
            }
        }
        if (startNetworkCommandLog.contains("network with name " + proxyNetwork + " already exists")) {
            String getNetworkSubnetCommand = "docker network inspect " + proxyNetwork;
            LogWriterComponents networkInspect = writeCommonLog(runShellCommand(getNetworkSubnetCommand), getNetworkSubnetCommand);
            String networkInspectLog = networkInspect.getLogWriter().toString();
            if(!networkInspectLog.contains("172.18.0.0")){
                setErrorLogs(networkInspect,  getNetworkSubnetCommand);
                throw new IOException("Network with name " + proxyNetwork + " exists, but it is set on different than 172.18.0.0 subnet! Whole log with emerged errors shown below " +
                        errorLog);
            }
            networkExistedBefore = true;
        }
        closeStreams(networkInitializerLogComponents);
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
        logger.info(writer.toString());
        log = log + "\n" + "Running command - \n" + command + "\n" + writer;
        return new LogWriterComponents(writer, errorWriter, istream, errorIStream, reader, errorReader);
    }

    protected static void closeStreams(LogWriterComponents logWriterComponents) throws IOException {
        logWriterComponents.getReader().close();
        logWriterComponents.getErrorReader().close();
        logWriterComponents.getIStream().close();
        logWriterComponents.getErrorIStream().close();
    }

    protected static String writeLogs(Process process, String command) throws IOException {
        LogWriterComponents logWriterComponents = writeCommonLog(process, command);
        Writer writer = logWriterComponents.getLogWriter();
        Writer errorWriter = logWriterComponents.getErrorLogWriter();
        if(!errorWriter.toString().equals("")){
            errorLog = errorLog + "\n" + "Running command - \n" + command + "\n" + errorWriter;
        }
        closeStreams(logWriterComponents);
        return writer.toString();
    }

    protected static void setErrorLogs(LogWriterComponents logWriterComponents, String command) {
        Writer errorWriter = logWriterComponents.getErrorLogWriter();
        if(!errorWriter.toString().equals("")){
            errorLog = errorLog + "\n" + "Running command - \n" + command + "\n" + errorWriter;
        }
    }

    protected static void addLog(String logLine){
        logger.info(logLine);
        log = log + "\n" + logLine + "\n";
    }

    protected static void addErrorLog(String errorLogLine){
        errorLog = errorLog + "\n" + errorLogLine + "\n";
    }
}
