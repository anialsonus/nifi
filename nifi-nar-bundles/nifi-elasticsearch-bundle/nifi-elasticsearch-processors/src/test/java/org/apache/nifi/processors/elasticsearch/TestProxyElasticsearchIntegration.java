package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.processors.elasticsearch.docker.ElasticsearchDockerInitializer;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.*;

import java.io.IOException;
import java.util.HashMap;

public class TestProxyElasticsearchIntegration extends ElasticsearchDockerInitializer {
    private TestRunner runner;
    private static byte[] docExample;

    static{
        squidUsed = true;
    }

    @BeforeClass
    public static void startElasticsearchSquidContainers() throws Exception {
        initializeElasticsearchSquidContainers();
    }


    @AfterClass
    public static  void terminateElasticsearchSquidContainers() throws Exception {
        clearElasticsearchSquidContainers();
    }

    @Before
    public void once() throws IOException {
        docExample = TestPutElasticsearchHttp.getDocExample();
    }

    @After
    public void teardown() {
        runner = null;
    }

    @Test
    public void testPutElasticsearchBasicBehindProxy() throws IOException {
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        runner = TestRunners.newTestRunner(new PutElasticsearchHttp());
        byte[] docExample = TestPutElasticsearchHttp.getDocExample();
        runner.setValidateExpressionUsage(false);

        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");

        runner.setProperty(PutElasticsearchHttp.PROXY_HOST, "localhost");
        runner.setProperty(PutElasticsearchHttp.PROXY_PORT, proxyPort);
        runner.setProperty(PutElasticsearchHttp.ES_URL, esUrlProxy);
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});

        runner.enqueue(docExample);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_SUCCESS, 1);
    }

    @Test
    public void testFetchElasticsearchBasicBehindProxy() throws IOException {
        testPutElasticsearchBasicBehindProxy();
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        runner = TestRunners.newTestRunner(new FetchElasticsearchHttp());
        runner.setValidateExpressionUsage(true);

        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");

        runner.setProperty(FetchElasticsearchHttp.PROXY_HOST, "localhost");
        runner.setProperty(FetchElasticsearchHttp.PROXY_PORT, proxyPort);
        runner.setProperty(FetchElasticsearchHttp.ES_URL, esUrlProxy);

        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});

        runner.enqueue(docExample);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_SUCCESS, 1);
    }

    @Test
    public void testQueryElasticsearchBasicBehindProxy() throws IOException, InterruptedException {
        testPutElasticsearchBasicBehindProxy();
        logger.info("Waiting for document to be put into elasticsearch...");
        Thread.sleep(10000);
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        runner = TestRunners.newTestRunner(new QueryElasticsearchHttp());

        runner.setProperty(QueryElasticsearchHttp.INDEX, "doc");
        runner.setProperty(QueryElasticsearchHttp.TYPE, "status");
        runner.setProperty(QueryElasticsearchHttp.QUERY, "${doc_id}");
        runner.setProperty(QueryElasticsearchHttp.FIELDS, "id,, userinfo.location");

        runner.setProperty(QueryElasticsearchHttp.PROXY_HOST, "localhost");
        runner.setProperty(QueryElasticsearchHttp.PROXY_PORT, proxyPort);
        runner.setProperty(QueryElasticsearchHttp.ES_URL, esUrlProxy);

        runner.enqueue("".getBytes(), new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});

        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(QueryElasticsearchHttp.REL_SUCCESS, 1);
    }

    @Test
    public void testPutElasticsearchBasicBehindAuthenticatedProxy() throws IOException {
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        runner = TestRunners.newTestRunner(new PutElasticsearchHttp());
        byte[] docExample = TestPutElasticsearchHttp.getDocExample();
        runner.setValidateExpressionUsage(false);

        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");
        runner.setProperty(PutElasticsearchHttp.ROUTING_ATTRIBUTE, "new_user");

        runner.setProperty(PutElasticsearchHttp.PROXY_HOST, "localhost");
        runner.setProperty(PutElasticsearchHttp.PROXY_PORT, proxyAuthPort);
        runner.setProperty(PutElasticsearchHttp.PROXY_USERNAME, "proxy-squid");
        runner.setProperty(PutElasticsearchHttp.PROXY_PASSWORD, "changeme");
        runner.setProperty(PutElasticsearchHttp.ES_URL, esUrlProxy);


        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});

        runner.enqueue(docExample);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_SUCCESS, 1);
    }

    @Test
    public void testFetchElasticsearchBasicBehindAuthenticatedProxy() throws IOException {
        testPutElasticsearchBasicBehindAuthenticatedProxy();
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        runner = TestRunners.newTestRunner(new FetchElasticsearchHttp());
        runner.setValidateExpressionUsage(true);

        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");

        runner.setProperty(FetchElasticsearchHttp.PROXY_HOST, "localhost");
        runner.setProperty(FetchElasticsearchHttp.PROXY_PORT, proxyAuthPort);
        runner.setProperty(FetchElasticsearchHttp.PROXY_USERNAME, "proxy-squid");
        runner.setProperty(FetchElasticsearchHttp.PROXY_PASSWORD, "changeme");
        runner.setProperty(FetchElasticsearchHttp.ES_URL, esUrlProxy);

        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});

        runner.enqueue(docExample);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_SUCCESS, 1);
    }

    @Test
    public void testQueryElasticsearchBasicBehindAuthenticatedProxy() throws IOException, InterruptedException {
        testPutElasticsearchBasicBehindAuthenticatedProxy();
        Thread.sleep(10000);
        logger.info("Waiting for document to be put into elasticsearch...");
        logger.info("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());

        runner = TestRunners.newTestRunner(new QueryElasticsearchHttp());
        runner.setValidateExpressionUsage(true);

        runner.setProperty(QueryElasticsearchHttp.INDEX, "doc");
        runner.setProperty(QueryElasticsearchHttp.TYPE, "status");
        runner.setProperty(QueryElasticsearchHttp.QUERY, "${doc_id}");
        runner.setProperty(QueryElasticsearchHttp.FIELDS, "id,, userinfo.location");

        runner.setProperty(QueryElasticsearchHttp.PROXY_HOST, "localhost");
        runner.setProperty(QueryElasticsearchHttp.PROXY_PORT, proxyAuthPort);
        runner.setProperty(QueryElasticsearchHttp.PROXY_USERNAME, "squid");
        runner.setProperty(QueryElasticsearchHttp.PROXY_PASSWORD, "changeme");
        runner.setProperty(QueryElasticsearchHttp.ES_URL, esUrlProxy);

        runner.enqueue("".getBytes(), new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});

        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(QueryElasticsearchHttp.REL_SUCCESS, 1);
    }


}
