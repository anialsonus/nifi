package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.processors.elasticsearch.docker.ElasticsearchDockerInitializer;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestElasticsearchIntegration extends ElasticsearchDockerInitializer {
    private TestRunner runner;
    private static byte[] docExample;
    private InputStream docExampleStream;


    static{
        squidUsed = false;
    }

    @BeforeClass
    public static void startElasticsearchContainers() throws Exception {
        initializeElasticsearchSquidContainers();
    }


    @AfterClass
    public static  void terminateElasticsearchContainers() throws Exception {
        clearElasticsearchSquidContainers();
    }



    @Before
    public void once() throws IOException {
        docExample = TestPutElasticsearchHttp.getDocExample();
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        docExampleStream = classloader.getResourceAsStream("DocumentExample.json");
    }

    @After
    public void teardown() {
        runner = null;
    }

    @Test
    public void testPutElasticsearchHttpRecordBasic() throws InitializationException {
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecord());
        MockRecordParser recordReader = new MockRecordParser();
        recordReader.addSchemaField("id", RecordFieldType.INT);
        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("code", RecordFieldType.INT);
        recordReader.addSchemaField("date", RecordFieldType.DATE);
        recordReader.addSchemaField("time", RecordFieldType.TIME);
        recordReader.addSchemaField("ts",RecordFieldType.TIMESTAMP);
        recordReader.addSchemaField("routing",RecordFieldType.STRING);

        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.setProperty(PutElasticsearchHttpRecord.RECORD_READER, "reader");
        runner.setProperty(PutElasticsearchHttpRecord.ES_URL, esUrl);
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchHttpRecord.ROUTING_RECORD_PATH,"/routing");
        runner.assertValid();
        Date date = new Date(new java.util.Date().getTime());
        Time time = new Time(System.currentTimeMillis());
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        recordReader.addRecord(0,"rec",100,date,time,timestamp,"user_0");

        runner.enqueue(new byte[0], new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});

        runner.enqueue(new byte[0]);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        List<ProvenanceEventRecord> provEvents = runner.getProvenanceEvents();
        assertNotNull(provEvents);
        assertEquals(1, provEvents.size());
        assertEquals(ProvenanceEventType.SEND, provEvents.get(0).getEventType());
    }

    @Test
    public void testPutElasticsearchHttpRecordBatch() throws InitializationException {
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecord());
        MockRecordParser recordReader = new MockRecordParser();
        recordReader.addSchemaField("id", RecordFieldType.INT);
        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("code", RecordFieldType.INT);
        recordReader.addSchemaField("date", RecordFieldType.DATE);
        recordReader.addSchemaField("time", RecordFieldType.TIME);
        recordReader.addSchemaField("ts",RecordFieldType.TIMESTAMP);
        recordReader.addSchemaField("routing",RecordFieldType.STRING);

        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.setProperty(PutElasticsearchHttpRecord.RECORD_READER, "reader");
        runner.setProperty(PutElasticsearchHttpRecord.ES_URL, esUrl);
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchHttpRecord.ROUTING_RECORD_PATH,"/routing");
        runner.assertValid();


        for (int i = 1; i < 101; i++) {
            Date date = new Date(new java.util.Date().getTime());
            Time time = new Time(System.currentTimeMillis());
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            recordReader.addRecord(i,"rec"+i,100+i,date,time,timestamp,"user_"+i);
            String newStrId = Integer.toString(i);
            runner.enqueue(new byte[0], new HashMap<String, String>() {{
                put("doc_id", newStrId);
            }});
            runner.run(1,true,true);
        }
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 100);
    }
    @Test
    public void testPutElasticsearchHttpBasic() throws IOException {
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner  runner = TestRunners.newTestRunner(new PutElasticsearchHttp());
        byte[] docExample = TestPutElasticsearchHttp.getDocExample();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, esUrl);
        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");
        runner.setProperty(PutElasticsearchHttp.ROUTING_ATTRIBUTE, "doc_routing");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
            put("doc_routing", "new_user");
        }});

        runner.enqueue(docExample);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_SUCCESS, 1);
    }

    @Test
    public void testFetchElasticsearchHttpBasic() throws IOException {
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new FetchElasticsearchHttp());
        testPutElasticsearchHttpBasic();
        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, esUrl);
        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});

        runner.enqueue(docExample);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_SUCCESS, 1);
    }

    @Test
    public void testPutElasticsearchHttpBatch() throws IOException {
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner  runner = TestRunners.newTestRunner(new PutElasticsearchHttp());
        byte[] docExample = TestPutElasticsearchHttp.getDocExample();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, esUrl);
        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "100");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");
        runner.setProperty(PutElasticsearchHttp.ROUTING_ATTRIBUTE, "doc_routing");

        runner.assertValid();

        for (int i = 0; i < 100; i++) {
            long newId = 28039652140L + i;
            final String newStrId = Long.toString(newId);
            int finalI = i;
            runner.enqueue(docExample, new HashMap<String, String>() {{
                put("doc_id", newStrId);
                put("doc_routing", "new_user" + finalI);
            }});
        }
        runner.run();
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_SUCCESS, 100);
    }

    @Test
    public void testFetchElasticsearchHttpBatch() throws IOException {
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        runner = TestRunners.newTestRunner(new FetchElasticsearchHttp());
        testPutElasticsearchHttpBatch();
        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, esUrl);
        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");
        runner.assertValid();

        for (int i = 0; i < 100; i++) {
            long newId = 28039652140L + i;
            final String newStrId = Long.toString(newId);
            runner.enqueue(docExample, new HashMap<String, String>() {{
                put("doc_id", newStrId);
            }});
        }
        runner.run(100);
        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_SUCCESS, 100);
    }

    @Test
    public void testPutElasticsearchTcpBasic() {
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new PutElasticsearch());

        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:"+esTcpPort);
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(PutElasticsearch.INDEX, "doc");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "1");

        runner.setProperty(PutElasticsearch.TYPE, "status");
        runner.setProperty(PutElasticsearch.ID_ATTRIBUTE, "doc_id");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});


        runner.enqueue(docExample);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearch.REL_SUCCESS, 1);
    }

    @Test
    public void testFetchElasticsearchTcpBasic() {
        testPutElasticsearchTcpBasic();
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new FetchElasticsearch());

        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:"+esTcpPort);
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(FetchElasticsearch.INDEX, "doc");

        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});


        runner.enqueue(docExample);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_SUCCESS, 1);
    }
    @Test
    public void testPutElasticsearchTcpBatch() throws IOException {
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new PutElasticsearch());

        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:"+esTcpPort);
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(PutElasticsearch.INDEX, "doc");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "100");

        runner.setProperty(PutElasticsearch.TYPE, "status");
        runner.setProperty(PutElasticsearch.ID_ATTRIBUTE, "doc_id");
        runner.assertValid();


        String message = convertStreamToString(docExampleStream);
        for (int i = 0; i < 100; i++) {

            long newId = 28039652140L + i;
            final String newStrId = Long.toString(newId);
            runner.enqueue(message.getBytes(), new HashMap<String, String>() {{
                put("doc_id", newStrId);
            }});

        }

        runner.run();

        runner.assertAllFlowFilesTransferred(PutElasticsearch.REL_SUCCESS, 100);
    }


    @Test
    public void testFetchElasticsearchTcpBatch() throws IOException {
        testPutElasticsearchTcpBatch();
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new FetchElasticsearch());

        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:"+esTcpPort);
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(FetchElasticsearch.INDEX, "doc");

        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");
        runner.assertValid();


        String message = convertStreamToString(docExampleStream);
        for (int i = 0; i < 100; i++) {

            long newId = 28039652140L + i;
            final String newStrId = Long.toString(newId);
            runner.enqueue(message.getBytes(), new HashMap<String, String>() {{
                put("doc_id", newStrId);
            }});

        }

        runner.run(100);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_SUCCESS, 100);
    }

    /**
     * Convert an input stream to a stream
     *
     * @param is input the input stream
     * @return return the converted input stream as a string
     */
    static String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
}