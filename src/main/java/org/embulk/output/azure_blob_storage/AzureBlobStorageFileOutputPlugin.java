package org.embulk.output.azure_blob_storage;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobType;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class AzureBlobStorageFileOutputPlugin
        implements FileOutputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("account_name")
        String getAccountName();

        // How to authenticate: account_key (default), service_principal or sas_token
        @Config("auth_method")
        @ConfigDefault("\"account_key\"")
        AuthMethod getAuthMethod();

        // Optional since auth_method was added. Required when auth_method is account_key.
        @Config("account_key")
        @ConfigDefault("null")
        Optional<String> getAccountKey();

        @Config("tenant_id")
        @ConfigDefault("null")
        Optional<String> getTenantId();

        @Config("client_id")
        @ConfigDefault("null")
        Optional<String> getClientId();

        @Config("client_secret")
        @ConfigDefault("null")
        Optional<String> getClientSecret();

        @Config("sas_token")
        @ConfigDefault("null")
        Optional<String> getSasToken();

        @Config("container")
        String getContainer();

        @Config("path_prefix")
        String getPathPrefix();

        @Config("file_ext")
        String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\"%03d.%02d\"")
        String getSequenceFormat();

        @Config("max_connection_retry")
        @ConfigDefault("10") // 10 times retry to connect Azure Blob Storage if failed.
        int getMaxConnectionRetry();

        /**
         * [BLOCK_BLOB | PAGE_BLOB | APPEND_BLOB | UNSPECIFIED]
         */
        @Config("blob_type")
        @ConfigDefault("\"UNSPECIFIED\"")
        String getBlobType();
    }

    private static final Logger log =  LoggerFactory.getLogger(AzureBlobStorageFileOutputPlugin.class);

    // Returned when the credentials are valid but not allowed to read the container's properties, e.g. a container
    // SAS without read permission or a role that allows only writing blobs. Writing may still be allowed.
    private static final List<String> PERMISSION_ERROR_CODES = Arrays.asList(
            "AuthorizationPermissionMismatch", "AuthorizationResourceTypeMismatch");

    public static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory
        .builder()
        .addDefaultModules()
        .build();
    public static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
                                  FileOutputPlugin.Control control)
    {
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        AzureBlobClientFactory.validate(task);

        createContainerIfNotExists(new AzureBlobClientFactory(task), task.getContainer());

        return resume(task.toTaskSource(), taskCount, control);
    }

    static void createContainerIfNotExists(AzureBlobClientFactory clientFactory, String containerName)
    {
        try {
            CloudBlobClient blobClient = clientFactory.createClient();
            CloudBlobContainer container = blobClient.getContainerReference(containerName);
            if (!container.exists(null, null, clientFactory.createOperationContext())) {
                log.info("container {} doesn't exist and is created.", containerName);
                container.createIfNotExists(null, null, clientFactory.createOperationContext());
            }
        }
        catch (StorageException ex) {
            if (ex.getHttpStatusCode() == 403 && PERMISSION_ERROR_CODES.contains(ex.getErrorCode())) {
                // Never returned for account_key, which can do everything. Uploading fails if writing is not allowed.
                log.warn("Skipped checking that container {} exists, because the credentials are not allowed to read it ({}).",
                        containerName, ex.getErrorCode());
            }
            else if (AzureErrors.findTokenRequestException(ex) != null) {
                // Microsoft Entra ID failed (e.g. temporarily), which is not a configuration error
                throw AzureErrors.toUserException(ex);
            }
            else {
                throw new ConfigException(AzureErrors.toUserException(ex).getMessage(), ex);
            }
        }
        catch (URISyntaxException ex) {
            throw new ConfigException(ex);
        }
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount, FileOutputPlugin.Control control)
    {
        control.run(taskSource);

        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount, List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex)
    {
        final PluginTask task = CONFIG_MAPPER_FACTORY.createTaskMapper().map(taskSource, PluginTask.class);
        final BlobType blobType = BlobType.valueOf(task.getBlobType());
        final AzureBlobClientFactory clientFactory = new AzureBlobClientFactory(task);
        final CloudBlobClient blobClient = clientFactory.createClient();

        // should support multiple blob type in the future.
        switch (blobType) {
            case BLOCK_BLOB:
                return new BlockBlobFileOutput(blobClient, clientFactory::createOperationContext, task, taskIndex, Exec.getTempFileSpace());
            default:
                return new BlobFileOutput(blobClient, clientFactory::createOperationContext, task, taskIndex, Exec.getTempFileSpace());
        }
    }
}
