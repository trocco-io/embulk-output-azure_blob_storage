package org.embulk.output.azure_blob_storage;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.spi.Buffer;
import org.embulk.spi.TempFileSpace;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.function.Supplier;

import static org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.PluginTask;

/**
 * Extract class from previous version of {@link AzureBlobStorageFileOutputPlugin} without logic modification
 */
public class BlobFileOutput implements TransactionalFileOutput
{
    private final Logger logger = LoggerFactory.getLogger(AzureBlobStorageFileOutputPlugin.class);
    private final CloudBlobClient client;
    // A new context for each request (null except for service_principal, which sets its access token through it)
    private final Supplier<OperationContext> operationContext;
    private final String containerName;
    private final String pathPrefix;
    private final String sequenceFormat;
    private final String pathSuffix;
    private final int maxConnectionRetry;
    private BufferedOutputStream output = null;
    private int fileIndex;
    private File file;
    private String filePath;
    private int taskIndex;
    private TempFileSpace tempFileSpace;

    public BlobFileOutput(CloudBlobClient client, PluginTask task, int taskIndex, final TempFileSpace tempFileSpace)
    {
        this(client, () -> null, task, taskIndex, tempFileSpace);
    }

    public BlobFileOutput(CloudBlobClient client, Supplier<OperationContext> operationContext, PluginTask task, int taskIndex,
                          final TempFileSpace tempFileSpace)
    {
        this.client = client;
        this.operationContext = operationContext;
        this.containerName = task.getContainer();
        this.taskIndex = taskIndex;
        this.pathPrefix = task.getPathPrefix();
        this.sequenceFormat = task.getSequenceFormat();
        this.pathSuffix = task.getFileNameExtension();
        this.maxConnectionRetry = task.getMaxConnectionRetry();
        this.tempFileSpace = tempFileSpace;
    }

    @Override
    public void nextFile()
    {
        closeFile();

        try {
            String suffix = pathSuffix;
            if (!suffix.startsWith(".")) {
                suffix = "." + suffix;
            }
            filePath = pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + suffix;
            file = this.tempFileSpace.createTempFile();
            logger.info("Writing local file {}", file.getAbsolutePath());
            output = new BufferedOutputStream(new FileOutputStream(file));
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void closeFile()
    {
        if (output != null) {
            try {
                output.close();
                fileIndex++;
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void add(Buffer buffer)
    {
        try {
            output.write(buffer.array(), buffer.offset(), buffer.limit());
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            buffer.release();
        }
    }

    @Override
    public void finish()
    {
        close();
        uploadFile();
    }

    private Void uploadFile()
    {
        if (filePath == null) {
            return null;
        }

        final Exception[] lastFailure = new Exception[1];
        try {
            return RetryExecutor.builder()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWaitMillis(500)
                    .withMaxRetryWaitMillis(30 * 1000)
                    .build()
                    .runInterruptible(new Retryable<Void>() {
                        @Override
                        public Void call() throws StorageException, URISyntaxException, IOException
                        {
                            CloudBlobContainer container = client.getContainerReference(containerName);
                            CloudBlockBlob blob = container.getBlockBlobReference(filePath);
                            logger.info("Upload start {} to {}", file.getAbsolutePath(), filePath);
                            try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
                                blob.upload(in, file.length(), null, null, operationContext.get());
                                logger.info("Upload completed {} to {}", file.getAbsolutePath(), filePath);
                            }
                            return null;
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            return AzureErrors.isRetryable(exception);
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                throws RetryGiveupException
                        {
                            if (exception instanceof FileNotFoundException || exception instanceof URISyntaxException || exception instanceof ConfigException) {
                                throw new RetryGiveupException(exception);
                            }
                            String message = String.format("Azure Blob Storage put request failed. Retrying %d/%d after %d seconds. Message: %s",
                                    retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                            if (retryCount % 3 == 0) {
                                logger.warn(message, exception);
                            }
                            else {
                                logger.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException)
                        {
                            lastFailure[0] = lastException;
                        }
                    });
        }
        catch (RetryGiveupException ex) {
            throw AzureErrors.giveUpError(ex.getCause(), lastFailure[0]);
        }
        catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            if (file.exists()) {
                if (!file.delete()) {
                    logger.warn("Couldn't delete local file " + file.getAbsolutePath());
                }
            }
        }
    }

    @Override
    public void close()
    {
        closeFile();
    }

    @Override
    public void abort() {}

    @Override
    public TaskReport commit()
    {
        return CONFIG_MAPPER_FACTORY.newTaskReport();
    }

    public boolean isTempFileExist()
    {
        return file.exists();
    }
}
