package org.embulk.output.azure_blob_storage;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import org.embulk.EmbulkSystemProperties;
import org.embulk.config.ConfigSource;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.Buffer;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FileOutputRunner;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TempFileSpace;
import org.embulk.spi.TempFileSpaceImpl;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.FileInputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

import static org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_ACCOUNT_KEY;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_ACCOUNT_NAME;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_CONTAINER;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_PATH_PREFIX;
import static org.embulk.output.azure_blob_storage.TestHelper.config;
import static org.embulk.output.azure_blob_storage.TestHelper.convertInputStreamToByte;
import static org.embulk.output.azure_blob_storage.TestHelper.getFileContentsFromAzure;
import static org.embulk.output.azure_blob_storage.TestHelper.newAzureClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

public class TestBlockBlobFileOutput
{
    private static final EmbulkSystemProperties EMBULK_SYSTEM_PROPERTIES = EmbulkSystemProperties.of(new Properties());
    private AzureBlobStorageFileOutputPlugin plugin;

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
        .setEmbulkSystemProperties(EMBULK_SYSTEM_PROPERTIES)
        .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
        .registerPlugin(FormatterPlugin.class, "csv", CsvFormatterPlugin.class)
        .registerPlugin(FileOutputPlugin.class, "azure_blob_storage", AzureBlobStorageFileOutputPlugin.class)
        .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
        .build();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private FileOutputRunner runner;

    @BeforeClass
    public static void init()
    {
        assumeNotNull(AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY, AZURE_CONTAINER);
    }

    @Before
    public void setup()
    {
        plugin = new AzureBlobStorageFileOutputPlugin();
        runner = new FileOutputRunner(plugin);
    }

    private ConfigSource getBlockBlobConfig()
    {
        return config().set("blob_type", "BLOCK_BLOB");
    }

    @Test
    public void testSingleBlock() throws Exception
    {
        ConfigSource configSource = getBlockBlobConfig();
        AzureBlobStorageFileOutputPlugin.PluginTask task = CONFIG_MAPPER.map(configSource, AzureBlobStorageFileOutputPlugin.PluginTask.class);
        Schema schema = CONFIG_MAPPER.map(configSource.getNested("parser"), CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        final CloudBlobClient blobClient = newAzureClient(task.getAccountName(), task.getAccountKey().get());
        final TempFileSpace tempFileSpace = TempFileSpaceImpl.with(testFolder.newFolder().toPath(), "output-azure-blob");

        BlockBlobFileOutput output = new BlockBlobFileOutput(blobClient, task, 0, tempFileSpace);
        output.nextFile();

        FileInputStream is = new FileInputStream(Resources.getResource("one_record.csv").getPath());
        byte[] bytes = convertInputStreamToByte(is);
        Buffer buffer = Buffer.wrap(bytes);
        output.add(buffer);

        output.finish();

        String remotePath = AZURE_PATH_PREFIX + String.format(task.getSequenceFormat(), 0, 0) + task.getFileNameExtension();

        ImmutableList<List<String>> records = getFileContentsFromAzure(remotePath);
        assertEquals(1, records.size());
        List<String> record = records.get(0);
        assertEquals("1", record.get(0));
        assertEquals("32864", record.get(1));
        assertEquals("2015-01-27 19:23:49", record.get(2));
        assertEquals("20150127", record.get(3));
        assertEquals("embulk", record.get(4));
        assertEquals("{\"k\":true}", record.get(5));
    }

    @Test
    public void testMultipleBlocks()
        throws Exception
    {
        ConfigSource configSource = getBlockBlobConfig();
        AzureBlobStorageFileOutputPlugin.PluginTask task = CONFIG_MAPPER.map(configSource, AzureBlobStorageFileOutputPlugin.PluginTask.class);
        Schema schema = CONFIG_MAPPER.map(configSource.getNested("parser"), CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        final CloudBlobClient blobClient = newAzureClient(task.getAccountName(), task.getAccountKey().get());

        final TempFileSpace tempFileSpace = TempFileSpaceImpl.with(testFolder.newFolder().toPath(), "output-azure-blob");

        BlockBlobFileOutput output = new BlockBlobFileOutput(blobClient, task, 0, tempFileSpace);

        // set small block size to check for multiple blocks upload
        Field blockSize = output.getClass().getDeclaredField("blockSize");
        blockSize.setAccessible(true);
        blockSize.set(output, 200); // flush for 200 bytes file

        output.nextFile();

        FileInputStream is = new FileInputStream(Resources.getResource("one_record.csv").getPath());
        byte[] bytes = convertInputStreamToByte(is);
        Buffer buffer = Buffer.wrap(bytes);
        for (int i = 0; i < 100; i++) {
            Field outputField = output.getClass().getDeclaredField("output");
            outputField.setAccessible(true);

            output.add(buffer);
            ((OutputStream) outputField.get(output)).flush();
        }

        output.finish();

        String remotePath = AZURE_PATH_PREFIX + String.format(task.getSequenceFormat(), 0, 0) + task.getFileNameExtension();

        ImmutableList<List<String>> records = getFileContentsFromAzure(remotePath);
        assertEquals(100, records.size());
        for (int i = 0; i < 100; i++) {
            List<String> record = records.get(i);
            assertEquals("1", record.get(0));
            assertEquals("32864", record.get(1));
            assertEquals("2015-01-27 19:23:49", record.get(2));
            assertEquals("20150127", record.get(3));
            assertEquals("embulk", record.get(4));
            assertEquals("{\"k\":true}", record.get(5));
        }
    }
}
