package org.embulk.output.azure_blob_storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.SendingRequestEvent;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageCredentialsToken;
import com.microsoft.azure.storage.StorageEvent;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import org.embulk.config.ConfigException;
import org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.PluginTask;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Creates the Azure Blob Storage client and the per-request context for the configured auth method.
 *
 * - account_key: uses the same connection string and credentials as before.
 * - sas_token: the SAS token is sent as the query string of each request.
 * - service_principal: an access token from Microsoft Entra ID is sent as a bearer token. The token is set right
 *   before each request is signed, so it is fetched inside each request attempt, and a token about to expire is
 *   refreshed during long transfers. A failed token request is not retried by the SDK (it fails with status 306), but
 *   by the plugin's retry of the upload (max_connection_retry).
 */
public class AzureBlobClientFactory
{
    private final String accountName;
    private final AuthMethod authMethod;
    private final String accountKey;
    private final String sasToken;
    private final ServicePrincipalTokenProvider tokenProvider;
    private StorageCredentialsToken tokenCredentials;

    // Tasks run in parallel in the same JVM. They share one token per service principal and secret instead of each
    // asking Entra ID for its own, which could hit its rate limit when many tasks start at once.
    private static final ConcurrentHashMap<String, ServicePrincipalTokenProvider> TOKEN_PROVIDERS = new ConcurrentHashMap<>();

    public AzureBlobClientFactory(PluginTask task)
    {
        this(task, task.getAuthMethod() == AuthMethod.SERVICE_PRINCIPAL ? sharedTokenProvider(task) : null);
    }

    AzureBlobClientFactory(PluginTask task, ServicePrincipalTokenProvider tokenProvider)
    {
        this.accountName = task.getAccountName();
        this.authMethod = task.getAuthMethod();
        // The account key is used as it is, as before. The new options are trimmed (see required()).
        this.accountKey = authMethod == AuthMethod.ACCOUNT_KEY ? requiredUntrimmed(task.getAccountKey(), "account_key", authMethod) : null;
        this.sasToken = authMethod == AuthMethod.SAS_TOKEN ? normalizeSasToken(required(task.getSasToken(), "sas_token", authMethod)) : null;
        this.tokenProvider = tokenProvider;
    }

    /**
     * Checks that the options needed by the auth method are set, before any request is sent.
     */
    public static void validate(PluginTask task)
    {
        new AzureBlobClientFactory(task, null);
        if (task.getAuthMethod() == AuthMethod.SERVICE_PRINCIPAL) {
            required(task.getTenantId(), "tenant_id", AuthMethod.SERVICE_PRINCIPAL);
            required(task.getClientId(), "client_id", AuthMethod.SERVICE_PRINCIPAL);
            required(task.getClientSecret(), "client_secret", AuthMethod.SERVICE_PRINCIPAL);
        }
    }

    public AuthMethod getAuthMethod()
    {
        return authMethod;
    }

    public CloudBlobClient createClient()
    {
        switch (authMethod) {
            case SERVICE_PRINCIPAL:
                return new CloudBlobClient(blobEndpoint(), tokenCredentials());
            case SAS_TOKEN:
                return new CloudBlobClient(blobEndpoint(), new StorageCredentialsSharedAccessSignature(sasToken));
            default:
                return newAccountKeyClient();
        }
    }

    /**
     * Returns a new context to pass to requests. It is null except for service_principal (null is what the requests
     * used before this class was added). The SDK keeps the result of every request in the context, so create a new one for each
     * request (or group of requests) instead of sharing one for a whole transfer.
     */
    public OperationContext createOperationContext()
    {
        if (authMethod != AuthMethod.SERVICE_PRINCIPAL) {
            return null;
        }
        OperationContext op = new OperationContext();
        final StorageCredentialsToken credentials = tokenCredentials();
        // Fired right before the request is signed, so the token set here is the one that is sent.
        op.getSendingRequestEventHandler().addListener(new StorageEvent<SendingRequestEvent>() {
            @Override
            public void eventOccurred(SendingRequestEvent event)
            {
                credentials.updateToken(tokenProvider.getToken());
            }
        });
        return op;
    }

    static ServicePrincipalTokenProvider sharedTokenProvider(PluginTask task)
    {
        final String tenantId = required(task.getTenantId(), "tenant_id", AuthMethod.SERVICE_PRINCIPAL);
        final String clientId = required(task.getClientId(), "client_id", AuthMethod.SERVICE_PRINCIPAL);
        final String clientSecret = required(task.getClientSecret(), "client_secret", AuthMethod.SERVICE_PRINCIPAL);
        // The secret is part of the key so that a changed secret gets a new token. The key stays in memory only.
        String key = String.join("\u0000", tenantId, clientId, clientSecret);
        return TOKEN_PROVIDERS.computeIfAbsent(key, k -> new ServicePrincipalTokenProvider(tenantId, clientId, clientSecret));
    }

    // Created without a token: createOperationContext() sets one before each request.
    private synchronized StorageCredentialsToken tokenCredentials()
    {
        if (tokenCredentials == null) {
            tokenCredentials = new StorageCredentialsToken(accountName, "");
        }
        return tokenCredentials;
    }

    private CloudBlobClient newAccountKeyClient()
    {
        String connectionString = "DefaultEndpointsProtocol=https;" +
                "AccountName=" + accountName + ";" +
                "AccountKey=" + accountKey;

        CloudStorageAccount account;
        try {
            account = CloudStorageAccount.parse(connectionString);
        }
        catch (InvalidKeyException | URISyntaxException ex) {
            throw new ConfigException(ex.getMessage());
        }
        return account.createCloudBlobClient();
    }

    URI blobEndpoint()
    {
        try {
            return new URI("https://" + accountName + ".blob.core.windows.net");
        }
        catch (URISyntaxException ex) {
            throw new ConfigException("Invalid 'account_name': " + accountName, ex);
        }
    }

    // The Azure portal shows a SAS token with a leading "?". The SDK's query parser tolerates it, but it is removed so
    // that the token is exactly the query string.
    static String normalizeSasToken(String sasToken)
    {
        String token = sasToken.trim();
        return token.startsWith("?") ? token.substring(1) : token;
    }

    // Values copied from the Azure portal easily get a trailing space or newline, which Entra ID and Azure reject.
    private static String required(Optional<String> value, String name, AuthMethod authMethod)
    {
        return requiredUntrimmed(value, name, authMethod).trim();
    }

    private static String requiredUntrimmed(Optional<String> value, String name, AuthMethod authMethod)
    {
        if (!value.isPresent() || value.get().trim().isEmpty()) {
            throw new ConfigException("'" + name + "' is required when 'auth_method' is '" + authMethod.getValue() + "'.");
        }
        return value.get();
    }
}
