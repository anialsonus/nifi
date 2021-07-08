/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.*;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class TestFetchElasticsearch {

    private InputStream docExample;
    private TestRunner runner;

    @Before
    public void setUp() throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        docExample = classloader.getResourceAsStream("DocumentExample.json");

    }

    @After
    public void teardown() {
        runner = null;
    }

    @Test
    public void testFetchElasticsearchOnTrigger() throws IOException {
        runner = TestRunners.newTestRunner(new FetchElasticsearchTestProcessor(true)); // all docs are found
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(FetchElasticsearch.INDEX, "doc");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_SUCCESS, 1);
        assertFalse(runner.getProvenanceEvents().isEmpty());
        runner.getProvenanceEvents().forEach(event -> assertEquals(event.getEventType(), ProvenanceEventType.FETCH) );
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearch.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testFetchElasticsearchOnTriggerEL() throws IOException {
        runner = TestRunners.newTestRunner(new FetchElasticsearchTestProcessor(true)); // all docs are found
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "${cluster.name}");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "${hosts}");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "${ping.timeout}");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "${sampler.interval}");

        runner.setProperty(FetchElasticsearch.INDEX, "doc");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");
        runner.assertValid();
        runner.setVariable("cluster.name", "elasticsearch");
        runner.setVariable("hosts", "127.0.0.1:9300");
        runner.setVariable("ping.timeout", "5s");
        runner.setVariable("sampler.interval", "5s");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearch.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testFetchElasticsearchOnTriggerWithFailures() throws IOException {
        runner = TestRunners.newTestRunner(new FetchElasticsearchTestProcessor(false)); // simulate doc not found
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(FetchElasticsearch.INDEX, "doc");
        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        // This test generates a "document not found"
        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_NOT_FOUND, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearch.REL_NOT_FOUND).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testFetchElasticsearchWithBadHosts() throws IOException {
        runner = TestRunners.newTestRunner(new FetchElasticsearchTestProcessor(false)); // simulate doc not found
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "http://127.0.0.1:9300,127.0.0.2:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(FetchElasticsearch.INDEX, "doc");
        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");

        runner.assertNotValid();
    }

    @Test
    public void testFetchElasticsearchOnTriggerWithExceptions() throws IOException {
        FetchElasticsearchTestProcessor processor = new FetchElasticsearchTestProcessor(true);
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(FetchElasticsearch.INDEX, "doc");
        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");

        // No Node Available exception
        processor.setExceptionToThrow(new NoNodeAvailableException("test"));
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_RETRY, 1);
        runner.clearTransferState();

        // Elasticsearch Timeout exception
        processor.setExceptionToThrow(new ElasticsearchTimeoutException("test"));
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652141");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_RETRY, 1);
        runner.clearTransferState();

        // Receive Timeout Transport exception
        processor.setExceptionToThrow(new ReceiveTimeoutTransportException(mock(StreamInput.class)));
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652141");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_RETRY, 1);
        runner.clearTransferState();

        // Node Closed exception
        processor.setExceptionToThrow(new NodeClosedException(mock(StreamInput.class)));
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652141");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_RETRY, 1);
        runner.clearTransferState();

        // Elasticsearch Parse exception
        processor.setExceptionToThrow(new ElasticsearchParseException("test"));
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652141");
        }});
        runner.run(1, true, true);

        // This test generates an exception on execute(),routes to failure
        runner.assertTransferCount(FetchElasticsearch.REL_FAILURE, 1);
    }

    @Test(expected = ProcessException.class)
    public void testCreateElasticsearchClientWithException() throws ProcessException {
        FetchElasticsearchTestProcessor processor = new FetchElasticsearchTestProcessor(true) {
            @Override
            protected TransportClient getTransportClient(Settings.Builder settingsBuilder, String shieldUrl,
                                                         String username, String password)
                    throws MalformedURLException {
                throw new MalformedURLException();
            }
        };

        MockProcessContext context = new MockProcessContext(processor);
        processor.initialize(new MockProcessorInitializationContext(processor, context));
        processor.callCreateElasticsearchClient(context);
    }

    @Test
    public void testSetupSecureClient() throws Exception {
        FetchElasticsearchTestProcessor processor = new FetchElasticsearchTestProcessor(true);
        runner = TestRunners.newTestRunner(processor);
        SSLContextService sslService = mock(SSLContextService.class);
        when(sslService.getIdentifier()).thenReturn("ssl-context");
        runner.addControllerService("ssl-context", sslService);
        runner.enableControllerService(sslService);
        runner.setProperty(FetchElasticsearch.PROP_SSL_CONTEXT_SERVICE, "ssl-context");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(FetchElasticsearch.INDEX, "doc");
        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");

        // Allow time for the controller service to fully initialize
        Thread.sleep(500);

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

    }

    /**
     * A Test class that extends the processor in order to inject/mock behavior
     */
    private static class FetchElasticsearchTestProcessor extends FetchElasticsearch {
        boolean documentExists = true;
        Exception exceptionToThrow = null;

        public FetchElasticsearchTestProcessor(boolean documentExists) {
            this.documentExists = documentExists;
        }

        public void setExceptionToThrow(Exception exceptionToThrow) {
            this.exceptionToThrow = exceptionToThrow;
        }

        @Override
        protected TransportClient getTransportClient(Settings.Builder settingsBuilder, String shieldUrl,
                                                     String username, String password)
                throws MalformedURLException {
            TransportClient mockClient = mock(TransportClient.class);
            GetRequestBuilder getRequestBuilder = spy(new GetRequestBuilder(mockClient, GetAction.INSTANCE));
            if (exceptionToThrow != null) {
                doThrow(exceptionToThrow).when(getRequestBuilder).execute();
            } else {
                doReturn(new MockGetRequestBuilderExecutor(documentExists)).when(getRequestBuilder).execute();
            }
            when(mockClient.prepareGet(anyString(), anyString(), anyString())).thenReturn(getRequestBuilder);

            return mockClient;
        }

        public void callCreateElasticsearchClient(ProcessContext context) {
            createElasticsearchClient(context);
        }

        private static class MockGetRequestBuilderExecutor
                extends AdapterActionFuture<GetResponse, ActionListener<GetResponse>>
                implements ListenableActionFuture<GetResponse> {

            boolean documentExists = true;

            public MockGetRequestBuilderExecutor(boolean documentExists) {
                this.documentExists = documentExists;
            }


            @Override
            protected GetResponse convert(ActionListener<GetResponse> bulkResponseActionListener) {
                return null;
            }

            @Override
            public void addListener(ActionListener<GetResponse> actionListener) {

            }

            @Override
            public GetResponse get() throws InterruptedException, ExecutionException {
                GetResponse response = mock(GetResponse.class);
                when(response.isExists()).thenReturn(documentExists);
                when(response.getSourceAsBytes()).thenReturn("Success".getBytes());
                when(response.getSourceAsString()).thenReturn("Success");
                return response;
            }

            @Override
            public GetResponse actionGet() {
                try {
                    return get();
                } catch (Exception e) {
                    fail(e.getMessage());
                }
                return null;
            }
        }
    }

}
