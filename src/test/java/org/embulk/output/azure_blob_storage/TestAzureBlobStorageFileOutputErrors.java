package org.embulk.output.azure_blob_storage;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryNoRetry;
import com.microsoft.azure.storage.SendingRequestEvent;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageCredentialsToken;
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
import org.embulk.spi.TransactionalFileOutput;
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
import java.util.function.IntFunction;

import static org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

// Tests that don't need an Azure account. A fake Blob service records the requests.
//
// The SDK sends a bearer token only over HTTPS, so the service_principal tests use the real context of
// AzureBlobClientFactory (which sets the token on the client's credentials right before each request) and add a
// listener after it that also sends that token as the x-test-token header of the plain HTTP request.
public class TestAzureBlobStorageFileOutputErrors
{
    private static final String SAS_TOKEN = "sv=1&sig=abc";

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private HttpServer server;
    // Responses to return in order: "status|errorCode". After they run out, 200 (HEAD), 202 (DELETE) or 201 is returned.
    private final List<String> queuedResponses = Collections.synchronizedList(new ArrayList<>());
    private final List<String> requests = Collections.synchronizedList(new ArrayList<>());
    private final List<String> queries = Collections.synchronizedList(new ArrayList<>());
    private final List<String> tokens = Collections.synchronizedList(new ArrayList<>());
    private final List<String> bodies = Collections.synchronizedList(new ArrayList<>());

    @Before
    public void startServer() throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String method = exchange.getRequestMethod();
            requests.add(method + " " + exchange.getRequestURI().getPath());
            queries.add(String.valueOf(exchange.getRequestURI().getRawQuery()));
            tokens.add(String.valueOf(exchange.getRequestHeaders().getFirst("x-test-token")));
            bodies.add(readAll(exchange.getRequestBody()));
            int status = "HEAD".equals(method) ? 200 : "DELETE".equals(method) ? 202 : 201;
            String errorCode = null;
            synchronized (queuedResponses) {
                if (!queuedResponses.isEmpty()) {
                    String[] response = queuedResponses.remove(0).split("\\|", 2);
                    status = Integer.parseInt(response[0]);
                    errorCode = response[1].isEmpty() ? null : response[1];
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

    // --- service_principal: the token is set on every request, through the path that open() uses

    @Test
    public void testServicePrincipalSetsTheTokenOnTheUpload() throws IOException
    {
        AtomicInteger calls = new AtomicInteger();
        PluginTask task = roundTrip(servicePrincipalTask(1, null));

        upload(servicePrincipalFactory(task, calls, call -> null), task, "a,b\n");

        assertEquals(Collections.singletonList("PUT /myaccount/my-container/data/000.00.csv"), requests);
        assertEquals(Collections.singletonList("token-1"), tokens);
        assertEquals("a,b\n", bodies.get(0));
    }

    @Test
    public void testServicePrincipalSetsTheTokenOnEachBlockRequest() throws IOException
    {
        AtomicInteger calls = new AtomicInteger();
        PluginTask task = roundTrip(servicePrincipalTask(1, "BLOCK_BLOB"));

        upload(servicePrincipalFactory(task, calls, call -> null), task, "a,b\n");

        // Put Block and Put Block List
        assertEquals(2, requests.size());
        assertEquals(Arrays.asList("token-1", "token-2"), tokens);
    }

    @Test
    public void testServicePrincipalSetsTheTokenOnEachContainerRequest()
    {
        // createIfNotExists() checks again before creating
        queuedResponses.add("404|ContainerNotFound");
        queuedResponses.add("404|ContainerNotFound");
        AtomicInteger calls = new AtomicInteger();

        AzureBlobStorageFileOutputPlugin.createContainerIfNotExists(
                servicePrincipalFactory(servicePrincipalTask(1, null), calls, call -> null), "my-container");

        assertEquals(Arrays.asList("HEAD /myaccount/my-container", "HEAD /myaccount/my-container", "PUT /myaccount/my-container"), requests);
        assertEquals(Arrays.asList("token-1", "token-2", "token-3"), tokens);
    }

    // The token is fetched right before the request, so a temporary error of Entra ID is retried with the upload
    @Test
    public void testTemporaryTokenErrorIsRetried() throws IOException
    {
        AtomicInteger calls = new AtomicInteger();
        PluginTask task = servicePrincipalTask(1, null);

        upload(servicePrincipalFactory(task, calls,
                call -> call == 1 ? new ServicePrincipalTokenProvider.RetryableTokenException("temporary", null) : null), task, "a,b\n");

        assertEquals(2, calls.get());
        assertEquals(Collections.singletonList("token-2"), tokens);
    }

    @Test
    public void testConfigErrorFromTokenIsNotRetried()
    {
        AtomicInteger calls = new AtomicInteger();
        PluginTask task = servicePrincipalTask(3, null);

        ConfigException ex = assertThrows(ConfigException.class, () -> upload(servicePrincipalFactory(task, calls,
                call -> new ConfigException("wrong client secret")), task, "a,b\n"));
        assertEquals("wrong client secret", ex.getMessage());
        assertEquals(1, calls.get());
        assertTrue(requests.isEmpty());
    }

    // e.g. an unexpected status from Entra ID
    @Test
    public void testTokenErrorThatIsNotTemporaryIsNotRetried()
    {
        AtomicInteger calls = new AtomicInteger();
        PluginTask task = servicePrincipalTask(3, null);

        DataException ex = assertThrows(DataException.class, () -> upload(servicePrincipalFactory(task, calls,
                call -> new ServicePrincipalTokenProvider.TokenRequestException("unexpected status", null)), task, "a,b\n"));
        assertEquals("unexpected status", ex.getMessage());
        assertEquals(1, calls.get());
    }

    // The retry library reports the first failure. The last one is shown, as it is why the retries stopped.
    @Test
    public void testTheLastFailureIsReported()
    {
        AtomicInteger calls = new AtomicInteger();
        PluginTask task = servicePrincipalTask(3, null);

        ConfigException ex = assertThrows(ConfigException.class, () -> upload(servicePrincipalFactory(task, calls,
                call -> call == 1 ? new ServicePrincipalTokenProvider.RetryableTokenException("temporary", null)
                        : new ConfigException("wrong client secret")), task, "a,b\n"));
        assertEquals("wrong client secret", ex.getMessage());
        assertEquals(2, calls.get());
    }

    // What the SDK really throws when getting the token fails: status 306, without sending the request
    @Test
    public void testFinalErrorOfAnUploadWhoseTokenRequestFailed()
    {
        PluginTask task = servicePrincipalTask(0, null);

        DataException ex = assertThrows(DataException.class, () -> upload(servicePrincipalFactory(task, new AtomicInteger(),
                call -> new ServicePrincipalTokenProvider.RetryableTokenException("Microsoft Entra ID returned a temporary error (status: 503)", null)),
                task, "a,b\n"));
        assertEquals("Microsoft Entra ID returned a temporary error (status: 503)", ex.getMessage());
        assertTrue(requests.isEmpty());
    }

    @Test
    public void testWrongClientSecretInTheContainerCheck()
    {
        ConfigException ex = assertThrows(ConfigException.class, () -> AzureBlobStorageFileOutputPlugin.createContainerIfNotExists(
                servicePrincipalFactory(servicePrincipalTask(1, null), new AtomicInteger(), call -> new ConfigException("wrong client secret")),
                "my-container"));
        assertEquals("wrong client secret", ex.getMessage());
        assertTrue(requests.isEmpty());
    }

    // A temporary error of Entra ID when the job starts is not reported as a configuration error
    @Test
    public void testTokenErrorInTheContainerCheckIsNotAConfigError()
    {
        RuntimeException ex = assertThrows(RuntimeException.class, () -> AzureBlobStorageFileOutputPlugin.createContainerIfNotExists(
                servicePrincipalFactory(servicePrincipalTask(1, null), new AtomicInteger(),
                        call -> new ServicePrincipalTokenProvider.RetryableTokenException("Microsoft Entra ID returned a temporary error (status: 503)", null)),
                "my-container"));
        assertFalse(ex.getClass().getName(), ex instanceof ConfigException);
        assertEquals("Microsoft Entra ID returned a temporary error (status: 503)", ex.getMessage());
    }

    // --- sas_token

    @Test
    public void testSasTokenIsSentAsTheQueryString() throws IOException
    {
        PluginTask task = roundTrip(sasTask(1, " ?" + SAS_TOKEN + "\n"));

        upload(factory(task), task, "a,b\n");

        assertEquals(1, requests.size());
        assertHasSasToken(queries.get(0));
        assertFalse(queries.get(0), queries.get(0).contains("??") || queries.get(0).contains("%0A") || queries.get(0).contains("%20"));
    }

    @Test
    public void testFinalErrorIncludesStatusCodeAndDetail()
    {
        queuedResponses.add("403|AuthenticationFailed");
        queuedResponses.add("403|AuthenticationFailed");
        PluginTask task = sasTask(1, SAS_TOKEN);

        DataException ex = assertThrows(DataException.class, () -> upload(factory(task), task, "a,b\n"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("status: 403, code: AuthenticationFailed"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("AuthenticationErrorDetail: Signature size is invalid. Check the credentials"));
        assertEquals(2, requests.size()); // the first try and one retry (max_connection_retry: 1)
    }

    @Test
    public void testFinalErrorOfAMissingRoleHasAHint()
    {
        queuedResponses.add("403|AuthorizationPermissionMismatch");
        PluginTask task = sasTask(0, SAS_TOKEN);

        DataException ex = assertThrows(DataException.class, () -> upload(factory(task), task, "a,b\n"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("Storage Blob Data Contributor"));
    }

    // Also returned when a firewall or network rule of the storage account blocks the request
    @Test
    public void testFinalErrorOfAuthorizationFailureMentionsTheFirewall()
    {
        queuedResponses.add("403|AuthorizationFailure");
        PluginTask task = sasTask(0, SAS_TOKEN);

        DataException ex = assertThrows(DataException.class, () -> upload(factory(task), task, "a,b\n"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("firewall"));
    }

    @Test
    public void testContainerIsCreatedWhenMissing()
    {
        queuedResponses.add("404|ContainerNotFound");
        queuedResponses.add("404|ContainerNotFound");

        AzureBlobStorageFileOutputPlugin.createContainerIfNotExists(factory(sasTask(1, SAS_TOKEN)), "my-container");

        assertEquals(Arrays.asList("HEAD /myaccount/my-container", "HEAD /myaccount/my-container", "PUT /myaccount/my-container"), requests);
        for (String query : queries) {
            assertHasSasToken(query);
        }
    }

    // A container SAS can never read the container's properties (AuthorizationFailure, checked with a real account).
    // An account SAS without the container resource type or the read permission gets the other two codes.
    @Test
    public void testContainerCheckIsSkippedWhenTheCredentialsCannotReadIt()
    {
        for (String code : new String[] {"AuthorizationFailure", "AuthorizationResourceTypeMismatch", "AuthorizationPermissionMismatch"}) {
            requests.clear();
            queuedResponses.add("403|" + code);

            AzureBlobStorageFileOutputPlugin.createContainerIfNotExists(factory(sasTask(1, SAS_TOKEN)), "my-container");

            assertEquals(code, 1, requests.size());
        }
    }

    // AuthorizationFailure is also returned when a firewall rule blocks the request: account_key fails as before
    @Test
    public void testContainerCheckIsNotSkippedForAccountKey()
    {
        queuedResponses.add("403|AuthorizationFailure");

        ConfigException ex = assertThrows(ConfigException.class,
                () -> AzureBlobStorageFileOutputPlugin.createContainerIfNotExists(factory(accountKeyTask()), "my-container"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("status: 403, code: AuthorizationFailure"));
    }

    @Test
    public void testContainerCheckFailsWithWrongCredentials()
    {
        queuedResponses.add("403|AuthenticationFailed");

        ConfigException ex = assertThrows(ConfigException.class,
                () -> AzureBlobStorageFileOutputPlugin.createContainerIfNotExists(factory(sasTask(1, SAS_TOKEN)), "my-container"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("status: 403, code: AuthenticationFailed"));
    }

    // e.g. allowed to write blobs, but not to create containers
    @Test
    public void testMissingContainerThatCannotBeCreatedFailsAtTheStart()
    {
        queuedResponses.add("404|ContainerNotFound");
        queuedResponses.add("404|ContainerNotFound");
        queuedResponses.add("403|AuthorizationPermissionMismatch");

        ConfigException ex = assertThrows(ConfigException.class,
                () -> AzureBlobStorageFileOutputPlugin.createContainerIfNotExists(factory(sasTask(1, SAS_TOKEN)), "my-container"));
        assertTrue(ex.getMessage(), ex.getMessage().startsWith("Container my-container doesn't exist, and the credentials are not allowed to create it."));
    }

    // --- account_key: no context and the same requests as before

    @Test
    public void testAccountKeyUploadHasNoContext() throws IOException
    {
        PluginTask task = roundTrip(accountKeyTask());
        AzureBlobClientFactory factory = factory(task);

        upload(factory, task, "a,b\n");

        assertNull(factory.createOperationContext());
        assertEquals(Collections.singletonList("PUT /myaccount/my-container/data/000.00.csv"), requests);
    }

    // --- BLOCK_BLOB

    @Test
    public void testBlockListCommitIsRetried() throws IOException
    {
        queuedResponses.add("201|");             // Put Block
        queuedResponses.add("503|ServerBusy");   // Put Block List
        PluginTask task = sasTask(1, "BLOCK_BLOB", SAS_TOKEN);

        upload(factory(task), task, "a,b\n");

        assertEquals(3, requests.size());
    }

    // abort() runs after the task failed: it deletes the uncommitted blocks, with the token on each request
    @Test
    public void testAbortDeletesTheUncommittedBlocks() throws IOException
    {
        AtomicInteger calls = new AtomicInteger();
        PluginTask task = servicePrincipalTask(1, "BLOCK_BLOB");
        TransactionalFileOutput output = AzureBlobStorageFileOutputPlugin.newOutput(
                servicePrincipalFactory(task, calls, call -> null), task, 0, tempFileSpace());
        output.nextFile();
        queuedResponses.add("404|BlobNotFound");

        output.abort();

        assertEquals(Arrays.asList("HEAD /myaccount/my-container/data/000.00.csv", "DELETE /myaccount/my-container/data/000.00.csv"), requests);
        assertEquals(Arrays.asList("token-1", "token-2"), tokens);
    }

    // e.g. a SAS token without the read or delete permission: the error of the task is the one to report
    @Test
    public void testAbortDoesNotFailWhenItCannotDelete() throws IOException
    {
        PluginTask task = sasTask(1, "BLOCK_BLOB", SAS_TOKEN);
        TransactionalFileOutput output = AzureBlobStorageFileOutputPlugin.newOutput(factory(task), task, 0, tempFileSpace());
        output.nextFile();
        queuedResponses.add("403|AuthorizationPermissionMismatch");

        output.abort();

        assertEquals(1, requests.size());
    }

    @Test
    public void testAbortBeforeAnyFileDoesNothing() throws IOException
    {
        PluginTask task = sasTask(1, "BLOCK_BLOB", SAS_TOKEN);
        AzureBlobStorageFileOutputPlugin.newOutput(factory(task), task, 0, tempFileSpace()).abort();
        assertTrue(requests.isEmpty());
    }

    // --- AzureErrors

    @Test
    public void testConfigErrorsAreNotRetried()
    {
        ConfigException configException = new ConfigException("wrong client secret");
        assertFalse(AzureErrors.isRetryable(configException));
        assertFalse(AzureErrors.isRetryable(new StorageException(null, "wrong client secret", 306, null, configException)));
        assertFalse(AzureErrors.isRetryable(new ServicePrincipalTokenProvider.TokenRequestException("unexpected status", null)));
        assertTrue(AzureErrors.isRetryable(new ServicePrincipalTokenProvider.RetryableTokenException("temporary", null)));
        // Everything else is retried as before.
        assertTrue(AzureErrors.isRetryable(new StorageException("AuthenticationFailed", "auth", 403, null, null)));
        assertTrue(AzureErrors.isRetryable(new RuntimeException("network")));
        assertSame(configException, AzureErrors.toUserException(new StorageException(null, "wrong client secret", 306, null, configException)));
    }

    // --- helpers

    // The SDK puts the parameters of the SAS token in its own order
    private static void assertHasSasToken(String query)
    {
        List<String> parameters = Arrays.asList(query.split("&"));
        assertTrue(query, parameters.contains("sv=1") && parameters.contains("sig=abc"));
    }

    private void upload(AzureBlobClientFactory factory, PluginTask task, String content) throws IOException
    {
        TransactionalFileOutput output = AzureBlobStorageFileOutputPlugin.newOutput(factory, task, 0, tempFileSpace());
        output.nextFile();
        output.add(Buffer.wrap(content.getBytes(StandardCharsets.UTF_8)));
        output.finish();
        output.commit();
    }

    private TempFileSpace tempFileSpace() throws IOException
    {
        return TempFileSpaceImpl.with(testFolder.newFolder().toPath(), "output-azure-blob");
    }

    private URI endpoint()
    {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/myaccount");
    }

    // A client for the fake service, without the SDK's own retries (so the plugin's retry is the one tested)
    private static CloudBlobClient withoutSdkRetries(CloudBlobClient client)
    {
        client.getDefaultRequestOptions().setRetryPolicyFactory(new RetryNoRetry());
        return client;
    }

    private AzureBlobClientFactory factory(PluginTask task)
    {
        return new AzureBlobClientFactory(task, null) {
            @Override
            URI blobEndpoint()
            {
                return endpoint();
            }

            @Override
            public CloudBlobClient createClient()
            {
                if (getAuthMethod() == AuthMethod.ACCOUNT_KEY) {
                    // The real one uses https://<account>.blob.core.windows.net from a connection string
                    return withoutSdkRetries(new CloudBlobClient(endpoint(), new StorageCredentialsAccountAndKey("myaccount", "a2V5")));
                }
                return withoutSdkRetries(super.createClient());
            }
        };
    }

    // The real service_principal factory and context. Requests are signed with SAS credentials (bearer tokens are sent
    // only over HTTPS), and the token that the real context set is sent as the x-test-token header.
    private AzureBlobClientFactory servicePrincipalFactory(PluginTask task, AtomicInteger calls, IntFunction<RuntimeException> error)
    {
        return new AzureBlobClientFactory(task, stubProvider(calls, error)) {
            @Override
            public CloudBlobClient createClient()
            {
                return withoutSdkRetries(new CloudBlobClient(endpoint(), new StorageCredentialsSharedAccessSignature(SAS_TOKEN)));
            }

            @Override
            public OperationContext createOperationContext()
            {
                OperationContext op = super.createOperationContext();
                final StorageCredentialsToken credentials = (StorageCredentialsToken) super.createClient().getCredentials();
                op.getSendingRequestEventHandler().addListener(new StorageEvent<SendingRequestEvent>() {
                    @Override
                    public void eventOccurred(SendingRequestEvent event)
                    {
                        ((HttpURLConnection) event.getConnectionObject()).setRequestProperty("x-test-token", credentials.getToken());
                    }
                });
                return op;
            }
        };
    }

    // open() reads the task back from the TaskSource
    private static PluginTask roundTrip(PluginTask task)
    {
        return CONFIG_MAPPER_FACTORY.createTaskMapper().map(task.toTaskSource(), PluginTask.class);
    }

    private static PluginTask servicePrincipalTask(int maxConnectionRetry, String blobType)
    {
        ConfigSource config = baseConfig(maxConnectionRetry).set("auth_method", "service_principal")
                .set("tenant_id", "t").set("client_id", "c").set("client_secret", "s");
        if (blobType != null) {
            config.set("blob_type", blobType);
        }
        return CONFIG_MAPPER.map(config, PluginTask.class);
    }

    private static PluginTask sasTask(int maxConnectionRetry, String sasToken)
    {
        return CONFIG_MAPPER.map(baseConfig(maxConnectionRetry).set("auth_method", "sas_token").set("sas_token", sasToken), PluginTask.class);
    }

    private static PluginTask sasTask(int maxConnectionRetry, String blobType, String sasToken)
    {
        return CONFIG_MAPPER.map(baseConfig(maxConnectionRetry).set("auth_method", "sas_token").set("sas_token", sasToken)
                .set("blob_type", blobType), PluginTask.class);
    }

    private static PluginTask accountKeyTask()
    {
        return CONFIG_MAPPER.map(baseConfig(1).set("account_key", "a2V5"), PluginTask.class);
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

    // Returns "token-N" for the N-th call, or throws the error that the function returns for that call
    private static ServicePrincipalTokenProvider stubProvider(AtomicInteger calls, IntFunction<RuntimeException> error)
    {
        return new ServicePrincipalTokenProvider("http://unused", "t", "c", "s", () -> 0L, millis -> { }) {
            @Override
            public synchronized String getToken()
            {
                int call = calls.incrementAndGet();
                RuntimeException ex = error.apply(call);
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
