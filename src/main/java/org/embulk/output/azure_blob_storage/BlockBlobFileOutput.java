package org.embulk.output.azure_blob_storage;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.BlockSearchMode;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.spi.Buffer;
import org.embulk.spi.DataException;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Supplier;

import static org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.CONFIG_MAPPER_FACTORY;

public class BlockBlobFileOutput implements TransactionalFileOutput
{
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final int blockSize;
    private final CloudBlobContainer container;
    // A new context for each request (null for account_key). service_principal sets its access token through it.
    private final Supplier<OperationContext> operationContext;
    private final String pathPrefix;
    private final String sequenceFormat;
    private final String pathSuffix;
    private final int maxConnectionRetry;
    private BufferedOutputStream output = null;
    private CloudBlockBlob blockBlob;
    private int fileIndex;
    private final int taskIndex;
    private File file;
    private int blockIndex = 0;
    private final List<BlockEntry> blocks = new ArrayList<>();
    private final TempFileSpace tempFileSpace;

    public BlockBlobFileOutput(CloudBlobClient client, AzureBlobStorageFileOutputPlugin.PluginTask task, int taskIndex, final TempFileSpace tempFileSpace)
    {
        this(client, () -> null, task, taskIndex, tempFileSpace);
    }

    public BlockBlobFileOutput(CloudBlobClient client, Supplier<OperationContext> operationContext,
                               AzureBlobStorageFileOutputPlugin.PluginTask task, int taskIndex, final TempFileSpace tempFileSpace)
    {
        this.operationContext = operationContext;
        try {
            this.container = client.getContainerReference(task.getContainer());
            this.tempFileSpace = tempFileSpace;
        }
        catch (Exception e) {
            throw new ConfigException(e);
        }
        this.taskIndex = taskIndex;
        this.pathPrefix = task.getPathPrefix();
        this.sequenceFormat = task.getSequenceFormat();
        this.pathSuffix = task.getFileNameExtension();
        this.maxConnectionRetry = task.getMaxConnectionRetry();
        // ~ 90M. init here for unit test changes it
        this.blockSize = 90 * 1024 * 1024;
    }

    @Override
    public void nextFile()
    {
        // close and commit current file
        closeCurrentFile();
        commitCurrentBlob();

        // prepare for next new file
        newTempFile();
        newBlockBlob();
        fileIndex++;
    }

    private void newBlockBlob()
    {
        try {
            blockBlob = container.getBlockBlobReference(newBlobName());
            blockIndex = 0;
            blocks.clear();
        }
        catch (Exception e) {
            throw new DataException(e);
        }
    }

    private String newBlobName()
    {
        String suffix = pathSuffix;
        if (!suffix.startsWith(".")) {
            suffix = "." + suffix;
        }
        return pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + suffix;
    }

    private void closeCurrentFile()
    {
        if (output != null) {
            try {
                output.close();
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private void newTempFile()
    {
        try {
            file = this.tempFileSpace.createTempFile();
            output = new BufferedOutputStream(new FileOutputStream(file));
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void add(Buffer buffer)
    {
        try {
            output.write(buffer.array(), buffer.offset(), buffer.limit());

            // upload this block if the size reaches limit (data can still in the buffer)
            if (file.length() > blockSize) {
                closeCurrentFile();
                uploadFile();
                newTempFile();
            }
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
        logger.info(">>> finish");
        closeCurrentFile();
        uploadFile();
        commitCurrentBlob();
    }

    private void commitCurrentBlob()
    {
        // commit blob
        if (!blocks.isEmpty()) {
            try {
                blockBlob.commitBlockList(blocks, null, null, operationContext.get());
                logger.info("Committed file: {}", blockBlob.getName());
                blocks.clear();
            }
            catch (StorageException e) {
                throw AzureErrors.toUserException(e);
            }
        }
    }

    private Void uploadFile()
    {
        if (file.length() == 0) {
            logger.warn("Skipped empty block {}", file.getName());
            return null;
        }

        try {
            return RetryExecutor.builder()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWaitMillis(500)
                    .withMaxRetryWaitMillis(30 * 1000)
                    .build()
                    .runInterruptible(new Retryable<Void>() {
                        @Override
                        public Void call() throws IOException, StorageException
                        {
                            String blockId = Base64.getEncoder().encodeToString(String.format("%10d", blockIndex).getBytes());
                            blockBlob.uploadBlock(blockId, new BufferedInputStream(new FileInputStream(file)), file.length(),
                                    null, null, operationContext.get());
                            blocks.add(new BlockEntry(blockId, BlockSearchMode.UNCOMMITTED));
                            logger.info("Uploaded block file: {}, id: {}, size ~ {}kb", file.getName(), blockId, file.length() / 1024);
                            blockIndex++;
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
                        }
                    });
        }
        catch (RetryGiveupException ex) {
            throw AzureErrors.toUserException(ex.getCause());
        }
        catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            if (file.exists() && !file.delete()) {
                logger.warn("Couldn't delete local file " + file.getAbsolutePath());
            }
        }
    }

    @Override
    public void close()
    {
        logger.info(">>> close");
        closeCurrentFile();
    }

    @Override
    public void abort()
    {
        try {
            // delete if blob not exist <=> delete all uncommitted blocks
            // if blob exist, leave it as is. uncommitted blocks will be garbage collected in 1 week.
            if (!blockBlob.exists(null, null, operationContext.get())) {
                blockBlob.delete(DeleteSnapshotsOption.NONE, null, null, operationContext.get());
            }
        }
        catch (StorageException e) {
            throw new DataException(e);
        }
    }

    @Override
    public TaskReport commit()
    {
        return CONFIG_MAPPER_FACTORY.newTaskReport();
    }
}
