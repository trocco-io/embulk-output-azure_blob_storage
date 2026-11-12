package org.embulk.output.azure_blob_storage;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageExtendedErrorInformation;
import org.embulk.config.ConfigException;
import org.embulk.spi.DataException;

/**
 * Error handling shared by the outputs: which errors to retry, and the error shown when a request finally fails.
 */
final class AzureErrors
{
    private AzureErrors()
    {
    }

    /**
     * Configuration errors (e.g. a wrong client secret) don't go away on retry. A token refresh that fails inside the
     * SDK comes back as a StorageException with the original exception as the cause, so the whole chain is checked.
     */
    static boolean isRetryable(Throwable exception)
    {
        return findConfigException(exception) == null;
    }

    /**
     * Makes the final error say why the request failed, instead of only the SDK's generic message.
     */
    static RuntimeException toUserException(Throwable cause)
    {
        ConfigException configException = findConfigException(cause);
        if (configException != null) {
            return configException;
        }
        if (cause instanceof StorageException) {
            StorageException storageException = (StorageException) cause;
            StringBuilder message = new StringBuilder(String.format("Azure Blob Storage request failed (status: %d, code: %s): %s",
                    storageException.getHttpStatusCode(), storageException.getErrorCode(), storageException.getMessage()));
            StorageExtendedErrorInformation details = storageException.getExtendedErrorInformation();
            if (details != null && details.getAdditionalDetails() != null) {
                details.getAdditionalDetails().forEach((key, values) ->
                        endSentence(message).append(" ").append(key).append(": ").append(String.join(", ", values)));
            }
            // When something fails inside the SDK before the request is sent (e.g. getting the access token), the SDK
            // still reports the status of the request it then sent without a token. Show the original error too.
            Throwable inner = storageException.getCause();
            if (inner != null && !(inner instanceof StorageException) && inner.getMessage() != null) {
                endSentence(message).append(" Caused by: ").append(inner.getMessage());
            }
            int status = storageException.getHttpStatusCode();
            if (status == 401 || status == 403) {
                endSentence(message).append(" Check the credentials for 'auth_method' (wrong or expired key or SAS token, or missing role or permission).");
            }
            return new DataException(message.toString(), storageException);
        }
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        return new RuntimeException(cause);
    }

    static ConfigException findConfigException(Throwable exception)
    {
        for (Throwable e = exception; e != null; e = e.getCause()) {
            if (e instanceof ConfigException) {
                return (ConfigException) e;
            }
            if (e.getCause() == e) {
                break;
            }
        }
        return null;
    }

    // The parts come from Azure and may not end with a period. Add one so that the next part reads as a new sentence.
    private static StringBuilder endSentence(StringBuilder message)
    {
        if (message.length() > 0 && message.charAt(message.length() - 1) != '.') {
            message.append('.');
        }
        return message;
    }
}
