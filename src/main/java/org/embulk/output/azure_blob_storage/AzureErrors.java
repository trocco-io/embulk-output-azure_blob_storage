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
     * Configuration errors (e.g. a wrong client secret) and errors of Entra ID that are not temporary don't go away on
     * retry. A token request that fails inside the SDK comes back as a StorageException with the original exception as
     * the cause, so the whole chain is checked. Other errors are retried, as before.
     */
    static boolean isRetryable(Throwable exception)
    {
        if (findConfigException(exception) != null) {
            return false;
        }
        ServicePrincipalTokenProvider.TokenRequestException tokenError = findTokenRequestException(exception);
        return tokenError == null || tokenError instanceof ServicePrincipalTokenProvider.RetryableTokenException;
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
        if (cause instanceof StorageException && failedBeforeSending((StorageException) cause)) {
            // The status and the message of such an exception are not from Azure, so show only the original error.
            return new DataException(cause.getCause().getMessage(), cause);
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
            // e.g. a network error while reading the response
            Throwable inner = storageException.getCause();
            if (inner != null && !(inner instanceof StorageException) && inner.getMessage() != null) {
                endSentence(message).append(" Caused by: ").append(inner.getMessage());
            }
            int status = storageException.getHttpStatusCode();
            if ("AuthorizationPermissionMismatch".equals(storageException.getErrorCode())
                    || "AuthorizationResourceTypeMismatch".equals(storageException.getErrorCode())) {
                endSentence(message).append(" The credentials are valid but not allowed to do this: give the service principal "
                        + "a data role that can write blobs (e.g. Storage Blob Data Contributor), or use a SAS token with the needed permissions.");
            }
            else if (status == 401 || status == 403) {
                endSentence(message).append(" Check the credentials for 'auth_method' (wrong or expired key or SAS token, or missing role or permission).");
            }
            return new DataException(message.toString(), storageException);
        }
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        return new RuntimeException(cause);
    }

    /**
     * The error to show after the retries gave up: the last failure, because the retry library reports the first one
     * (e.g. a temporary error first, and then a wrong client secret that is not retried).
     */
    static RuntimeException giveUpError(Throwable firstFailure, Throwable lastFailure)
    {
        Throwable failure = lastFailure != null ? lastFailure : firstFailure;
        RuntimeException error = toUserException(failure);
        if (firstFailure != null && firstFailure != failure && error != firstFailure) {
            error.addSuppressed(firstFailure);
        }
        return error;
    }

    /**
     * True when the request was not sent, because something failed while preparing or signing it: the SDK catches
     * such an exception (e.g. one thrown while getting the access token) and throws a StorageException with status 306
     * and the original exception as the cause.
     */
    static boolean failedBeforeSending(StorageException exception)
    {
        return exception.getHttpStatusCode() == 306 && exception.getCause() != null
                && !(exception.getCause() instanceof StorageException) && exception.getCause().getMessage() != null;
    }

    /**
     * Returns the error of getting the access token in the cause chain, if any (other than a configuration error).
     */
    static ServicePrincipalTokenProvider.TokenRequestException findTokenRequestException(Throwable exception)
    {
        for (Throwable e = exception; e != null; e = e.getCause()) {
            if (e instanceof ServicePrincipalTokenProvider.TokenRequestException) {
                return (ServicePrincipalTokenProvider.TokenRequestException) e;
            }
            if (e.getCause() == e) {
                break;
            }
        }
        return null;
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
