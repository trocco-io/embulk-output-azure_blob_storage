package org.embulk.output.azure_blob_storage;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryNoRetry;
import com.microsoft.azure.storage.SendingRequestEvent;
import com.microsoft.azure.storage.StorageEvent;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.sun.net.httpserver.HttpServer;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.PluginTask;
import org.embulk.spi.Buffer;
import org.embulk.spi.DataException;
import org.embulk.spi.TempFileSpace;
import org.embulk.spi.TempFileSpaceImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

// Tests that don't need an Azure account. A fake Blob service records the requests.
public class TestAzureBlobStorageFileOutputErrors
{
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private HttpServer server;
    // Responses to return in order: "status|errorCode". After they run out, 201 Created (or 200 for HEAD) is returned.
    private final List<String> queuedResponses = Collections.synchronizedList(new ArrayList<>());
    private final List<String> requests = Collections.synchronizedList(new ArrayList<>());
    private final List<String> tokens = Collections.synchronizedList(new ArrayList<>());
    private final List<String> bodies = Collections.synchronizedList(new ArrayList<>());

    @Before
    public void startServer() throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String method = exchange.getRequestMethod();
            requests.add(method + " " + exchange.getRequestURI().getPath());
            tokens.add(String.valueOf(exchange.getRequestHeaders().getFirst("x-test-token")));
            bodies.add(readAll(exchange.getRequestBody()));
            int status = "HEAD".equals(method) ? 200 : 201;
            String errorCode = null;
            synchronized (queuedResponses) {
                if (!queuedResponses.isEmpty()) {
                    String[] response = queuedResponses.remove(0).split("\\|", 2);
                    status = Integer.parseInt(response[0]);
                    errorCode = response[1];
                }
            }
            exchange.getResponseHeaders().add("ETag", "\"0x1\"");
            exchange.getResponseHeaders().add("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT");
            if (errorCode != null) {
                // HEAD responses have no body, so Azure sends the error code in this header
                exchange.getResponseHeaders().add("x-ms-error-code", errorCode);
            }
            byte[] body = errorCode == null || "HEAD".equals(method) ? new byte[0]
                    : ("<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>" + errorCode + "</Code><Message>failed</Message>"
                    + "<AuthenticationErrorDetail>Signature size is invalid</AuthenticationErrorDetail></Error>").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/xml");
            exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
            if (body.length > 0) {
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
            }
            exchange.close();
        });
        server.start();
    }

    @After
    public void stopServer()
    {
        server.stop(0);
    }

    // The SDK sends a bearer token only over HTTPS, so these tests stamp the token as a header of the plain HTTP
    // request instead, from the same SendingRequest event that AzureBlobClientFactory uses to set the token.

    @Test
    public void testTheTokenIsSetRightBeforeTheUpload() throws IOException
    {
        AtomicInteger calls = new AtomicInteger();
        PluginTask task = sasTask(1);

        upload(factory(task, null), tokenContext(stubProvider(calls, () -> null)), task, "a,b\n");

        assertEquals(Collections.singletonList("PUT /myaccount/my-container/data/000.00.csv"), requests);
        assertEquals(Collections.singletonList("token-1"), tokens);
        assertEquals("a,b\n", bodies.get(0));
    }

    // The token is fetched right before the request, so a temporary error of Entra ID is retried with the upload
    @Test
    public void testTemporaryTokenErrorIsRetried() throws IOException
    {
        AtomicInteger calls = new AtomicInteger();
        PluginTask task = sasTask(1);
        Supplier<OperationContext> context = tokenContext(stubProvider(calls,
                () -> calls.get() == 1 ? new ServicePrincipalTokenProvider.RetryableTokenException("temporary", null) : null));

        upload(factory(task, null), context, task, "a,b\n");

        assertEquals(2, calls.get());
        assertEquals(Collections.singletonList("token-2"), tokens);
    }

    @Test
    public void testConfigErrorFromTokenIsNotRetried()
    {
        AtomicInteger calls = new AtomicInteger();
        PluginTask task = sasTask(3);
        Supplier<OperationContext> context = tokenContext(stubProvider(calls, () -> new ConfigException("wrong client secret")));

        ConfigException ex = assertThrows(ConfigException.class, () -> upload(factory(task, null), context, task, "a,b\n"));
        assertEquals("wrong client secret", ex.getMessage());
        assertEquals(1, calls.get());
        assertTrue(requests.isEmpty());
    }

    @Test
    public void testFinalErrorIncludesStatusCodeAndDetail()
    {
        queuedResponses.add("403|AuthenticationFailed");
        queuedResponses.add("403|AuthenticationFailed");
        PluginTask task = sasTask(1);

        DataException ex = assertThrows(DataException.class, () -> upload(factory(task, null), task, "a,b\n"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("status: 403, code: AuthenticationFailed"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("AuthenticationErrorDetail: Signature size is invalid. Check the credentials"));
        assertEquals(2, requests.size()); // the first try and one retry (max_connection_retry: 1)
    }

    @Test
    public void testBlockBlobUploadSetsTheTokenForEachRequest() throws IOException
    {
        AtomicInteger calls = new AtomicInteger();
        PluginTask task = sasTask(1);
        AzureBlobClientFactory factory = factory(task, null);
        TempFileSpace tempFileSpace = TempFileSpaceImpl.with(testFolder.newFolder().toPath(), "output-azure-blob");
        BlockBlobFileOutput output = new BlockBlobFileOutput(factory.createClient(), tokenContext(stubProvider(calls, () -> null)),
                task, 0, tempFileSpace);
        output.nextFile();
        output.add(Buffer.wrap("a,b\n".getBytes(StandardCharsets.UTF_8)));
        output.finish();
        output.commit();

        // Put Block and Put Block List
        assertEquals(2, requests.size());
        assertEquals(Arrays.asList("token-1", "token-2"), tokens);
    }

    @Test
    public void testContainerIsCreatedWhenMissing()
    {
        // createIfNotExists() checks again before creating
        queuedResponses.add("404|ContainerNotFound");
        queuedResponses.add("404|ContainerNotFound");
        PluginTask task = sasTask(1);

        AzureBlobStorageFileOutputPlugin.createContainerIfNotExists(factory(task, null), "my-container");

        assertEquals(Arrays.asList("HEAD /myaccount/my-container", "HEAD /myaccount/my-container",
                "PUT /myaccount/my-container"), requests);
    }

    // e.g. a container SAS without read permission, or a role that only allows writing blobs
    @Test
    public void testContainerCheckIsSkippedWithoutReadPermission()
    {
        queuedResponses.add("403|AuthorizationPermissionMismatch");
        PluginTask task = sasTask(1);

        AzureBlobStorageFileOutputPlugin.createContainerIfNotExists(factory(task, null), "my-container");

        assertEquals(1, requests.size());
    }

    @Test
    public void testContainerCheckFailsWithWrongCredentials()
    {
        queuedResponses.add("403|AuthenticationFailed");
        PluginTask task = sasTask(1);

        ConfigException ex = assertThrows(ConfigException.class,
                () -> AzureBlobStorageFileOutputPlugin.createContainerIfNotExists(factory(task, null), "my-container"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("status: 403, code: AuthenticationFailed"));
    }

    @Test
    public void testConfigErrorsAreNotRetried()
    {
        ConfigException configException = new ConfigException("wrong client secret");
        assertFalse(AzureErrors.isRetryable(configException));
        assertFalse(AzureErrors.isRetryable(new StorageException("AuthenticationFailed", "auth", 401, null, configException)));
        assertTrue(AzureErrors.isRetryable(new StorageException("AuthenticationFailed", "auth", 403, null, null)));
        assertTrue(AzureErrors.isRetryable(new RuntimeException("network")));
        assertSame(configException, AzureErrors.toUserException(new StorageException("AuthenticationFailed", "auth", 401, null, configException)));
    }

    // The SDK catches an exception thrown before signing and throws this, without sending the request
    @Test
    public void testFinalErrorOfAFailedTokenRequestShowsOnlyTheOriginalError()
    {
        RuntimeException tokenError = new ServicePrincipalTokenProvider.RetryableTokenException(
                "Microsoft Entra ID returned a temporary error (status: 503)", null);
        RuntimeException ex = AzureErrors.toUserException(new StorageException(null, tokenError.getMessage(), 306, null, tokenError));
        assertTrue(ex instanceof DataException);
        assertEquals("Microsoft Entra ID returned a temporary error (status: 503)", ex.getMessage());
    }

    // What the SDK really throws when the listener fails, checked through an upload
    @Test
    public void testFinalErrorOfAnUploadWhoseTokenRequestFailed()
    {
        PluginTask task = sasTask(0);
        Supplier<OperationContext> context = tokenContext(stubProvider(new AtomicInteger(),
                () -> new ServicePrincipalTokenProvider.RetryableTokenException("Microsoft Entra ID returned a temporary error (status: 503)", null)));

        DataException ex = assertThrows(DataException.class, () -> upload(factory(task, null), context, task, "a,b\n"));
        assertEquals("Microsoft Entra ID returned a temporary error (status: 503)", ex.getMessage());
        assertTrue(requests.isEmpty());
    }

    // A temporary error of Entra ID when the job starts is not reported as a configuration error
    @Test
    public void testTokenErrorInTheContainerCheckIsNotAConfigError()
    {
        PluginTask task = sasTask(0);
        AzureBlobClientFactory factory = new AzureBlobClientFactory(task, null) {
            @Override
            URI blobEndpoint()
            {
                return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/myaccount");
            }

            @Override
            public OperationContext createOperationContext()
            {
                return tokenContext(stubProvider(new AtomicInteger(),
                        () -> new ServicePrincipalTokenProvider.RetryableTokenException("Microsoft Entra ID returned a temporary error (status: 503)", null))).get();
            }
        };

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> AzureBlobStorageFileOutputPlugin.createContainerIfNotExists(factory, "my-container"));
        assertFalse(ex.getClass().getName(), ex instanceof ConfigException);
        assertEquals("Microsoft Entra ID returned a temporary error (status: 503)", ex.getMessage());
    }

    private void upload(AzureBlobClientFactory factory, PluginTask task, String content) throws IOException
    {
        upload(factory, factory::createOperationContext, task, content);
    }

    private void upload(AzureBlobClientFactory factory, Supplier<OperationContext> context, PluginTask task, String content)
            throws IOException
    {
        TempFileSpace tempFileSpace = TempFileSpaceImpl.with(testFolder.newFolder().toPath(), "output-azure-blob");
        BlobFileOutput output = new BlobFileOutput(factory.createClient(), context, task, 0, tempFileSpace);
        output.nextFile();
        output.add(Buffer.wrap(content.getBytes(StandardCharsets.UTF_8)));
        output.finish();
        output.commit();
    }

    // The client of the factory sends requests to the fake service, without the SDK's own retries
    private AzureBlobClientFactory factory(PluginTask task, ServicePrincipalTokenProvider provider)
    {
        URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/myaccount");
        return new AzureBlobClientFactory(task, provider) {
            @Override
            URI blobEndpoint()
            {
                return endpoint;
            }

            @Override
            public CloudBlobClient createClient()
            {
                CloudBlobClient client = super.createClient();
                client.getDefaultRequestOptions().setRetryPolicyFactory(new RetryNoRetry());
                return client;
            }
        };
    }

    // A new context for each request, as AzureBlobClientFactory.createOperationContext() returns for service_principal
    private static Supplier<OperationContext> tokenContext(ServicePrincipalTokenProvider provider)
    {
        return () -> {
            OperationContext op = new OperationContext();
            op.getSendingRequestEventHandler().addListener(new StorageEvent<SendingRequestEvent>() {
                @Override
                public void eventOccurred(SendingRequestEvent event)
                {
                    ((HttpURLConnection) event.getConnectionObject()).setRequestProperty("x-test-token", provider.getToken());
                }
            });
            return op;
        };
    }

    private PluginTask sasTask(int maxConnectionRetry)
    {
        return CONFIG_MAPPER.map(baseConfig(maxConnectionRetry).set("auth_method", "sas_token").set("sas_token", "sv=1&sig=abc"),
                PluginTask.class);
    }

    private static ConfigSource baseConfig(int maxConnectionRetry)
    {
        return CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("account_name", "myaccount")
                .set("container", "my-container")
                .set("path_prefix", "data/")
                .set("file_ext", ".csv")
                .set("max_connection_retry", maxConnectionRetry);
    }

    // Returns "token-N" for the N-th call, or throws the error that the supplier returns for that call
    private static ServicePrincipalTokenProvider stubProvider(AtomicInteger calls, Supplier<RuntimeException> error)
    {
        return new ServicePrincipalTokenProvider("http://unused", "t", "c", "s", () -> 0L, millis -> { }) {
            @Override
            public synchronized String getToken()
            {
                int call = calls.incrementAndGet();
                RuntimeException ex = error.get();
                if (ex != null) {
                    throw ex;
                }
                return "token-" + call;
            }
        };
    }

    private static String readAll(InputStream in) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
}
