package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.processors.elasticsearch.docker.DockerComposeContainerType;
import org.apache.nifi.processors.elasticsearch.docker.ElasticsearchDockerInitializer;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.*;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.HashMap;

public class TestElasticsearchIntegrationProxy {
    private TestRunner runner;
    private static DockerComposeContainer elasticsearchDockerComposeContainer = ElasticsearchDockerInitializer.initiateElasticsearchDockerComposeContainer(DockerComposeContainerType.PROXY);
    private static byte[] docExample;
    private String esUrl = "http://172.18.0.2:"+elasticsearchDockerComposeContainer.getServicePort("es01", 9200);

    @Before
    public void once() throws IOException {
        docExample = TestPutElasticsearchHttp.getDocExample();
    }

    @After
    public void teardown() {
        runner = null;
    }

    @BeforeClass
    public static void startElasticSearchDockerContainer(){
        elasticsearchDockerComposeContainer.start();
    }
    @AfterClass
    public static void stopElasticSearchDockerContainer(){
        elasticsearchDockerComposeContainer.stop();
        elasticsearchDockerComposeContainer.close();
    }

    @Test
    public void testPutElasticSearchBasicBehindProxy() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutElasticsearchHttp());
        byte[] docExample = TestPutElasticsearchHttp.getDocExample();
        runner.setValidateExpressionUsage(false);

        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");

        runner.setProperty(PutElasticsearchHttp.PROXY_HOST, "localhost");
        runner.setProperty(PutElasticsearchHttp.PROXY_PORT, "3228");
        runner.setProperty(PutElasticsearchHttp.ES_URL, esUrl);
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
        testPutElasticSearchBasicBehindProxy();
        runner = TestRunners.newTestRunner(new FetchElasticsearchHttp());
        runner.setValidateExpressionUsage(true);

        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");

        runner.setProperty(FetchElasticsearchHttp.PROXY_HOST, "localhost");
        runner.setProperty(FetchElasticsearchHttp.PROXY_PORT, "3228");
        runner.setProperty(FetchElasticsearchHttp.ES_URL, esUrl);

        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});

        runner.enqueue(docExample);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_SUCCESS, 1);
    }
    @Test
    public void testPutElasticSearchBasicBehindAuthenticatedProxy() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutElasticsearchHttp());
        byte[] docExample = TestPutElasticsearchHttp.getDocExample();
        runner.setValidateExpressionUsage(false);

        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");
        runner.setProperty(PutElasticsearchHttp.ROUTING_ATTRIBUTE, "new_user");

        runner.setProperty(PutElasticsearchHttp.PROXY_HOST, "localhost");
        runner.setProperty(PutElasticsearchHttp.PROXY_PORT, "3328");
        runner.setProperty(PutElasticsearchHttp.PROXY_USERNAME, "proxy-squid");
        runner.setProperty(PutElasticsearchHttp.PROXY_PASSWORD, "changeme");
        runner.setProperty(PutElasticsearchHttp.ES_URL, esUrl);


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
        testPutElasticSearchBasicBehindAuthenticatedProxy();
        runner = TestRunners.newTestRunner(new FetchElasticsearchHttp());
        runner.setValidateExpressionUsage(true);

        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");

        runner.setProperty(FetchElasticsearchHttp.PROXY_HOST, "localhost");
        runner.setProperty(FetchElasticsearchHttp.PROXY_PORT, "3328");
        runner.setProperty(FetchElasticsearchHttp.PROXY_USERNAME, "proxy-squid");
        runner.setProperty(FetchElasticsearchHttp.PROXY_PASSWORD, "changeme");
        runner.setProperty(FetchElasticsearchHttp.ES_URL, esUrl);

        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});

        runner.enqueue(docExample);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_SUCCESS, 1);
    }
}
