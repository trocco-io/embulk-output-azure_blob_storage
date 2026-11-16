package org.embulk.output.azure_blob_storage;

import com.google.common.collect.Lists;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.PluginTask;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FileOutputRunner;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;

import static org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_ACCOUNT_KEY;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_ACCOUNT_NAME;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_CONTAINER;
import static org.embulk.output.azure_blob_storage.TestHelper.config;
import static org.embulk.output.azure_blob_storage.TestHelper.deleteContainerIfExists;
import static org.embulk.output.azure_blob_storage.TestHelper.formatterConfig;
import static org.embulk.output.azure_blob_storage.TestHelper.getSchema;
import static org.embulk.output.azure_blob_storage.TestHelper.inputConfig;
import static org.embulk.output.azure_blob_storage.TestHelper.isExistsContainer;
import static org.embulk.output.azure_blob_storage.TestHelper.parserConfig;
import static org.embulk.output.azure_blob_storage.TestHelper.schemaConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

public class TestAzureBlobStorageFileOutputPlugin
{
    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
        .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
        .registerPlugin(FormatterPlugin.class, "csv", CsvFormatterPlugin.class)
        .registerPlugin(FileOutputPlugin.class, "azure_blob_storage", AzureBlobStorageFileOutputPlugin.class)
        .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
        .build();

    private FileOutputRunner runner;

    @BeforeClass
    public static void init()
    {
        assumeNotNull(AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY, AZURE_CONTAINER);
    }

    @Before
    public void createResources()
        throws GeneralSecurityException, NoSuchMethodException, IOException
    {
        AzureBlobStorageFileOutputPlugin plugin = new AzureBlobStorageFileOutputPlugin();
        runner = new FileOutputRunner(plugin);
    }

    @Test
    public void checkDefaultValues()
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                                  .set("in", inputConfig())
                                  .set("parser", parserConfig(schemaConfig()))
                                  .set("type", "azure_blob_storage")
                                  .set("account_name", AZURE_ACCOUNT_NAME)
                                  .set("account_key", AZURE_ACCOUNT_KEY)
                                  .set("container", AZURE_CONTAINER)
                                  .set("path_prefix", "my-prefix")
                                  .set("file_ext", ".csv")
                                  .set("formatter", formatterConfig());

        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        assertEquals(AZURE_ACCOUNT_NAME, task.getAccountName());
        assertEquals(AZURE_ACCOUNT_KEY, task.getAccountKey().get());
        assertEquals(AZURE_CONTAINER, task.getContainer());
        assertEquals(10, task.getMaxConnectionRetry());
    }

    @Test
    public void testTransaction()
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                                  .set("in", inputConfig())
                                  .set("parser", parserConfig(schemaConfig()))
                                  .set("type", "azure_blob_storage")
                                  .set("account_name", AZURE_ACCOUNT_NAME)
                                  .set("account_key", AZURE_ACCOUNT_KEY)
                                  .set("container", AZURE_CONTAINER)
                                  .set("file_ext", ".csv")
                                  .set("path_prefix", "my-prefix")
                                  .set("formatter", formatterConfig());

        Schema schema = CONFIG_MAPPER.map(config.getNested("parser"), CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        try {
            TestingEmbulk.RunResult result = embulk.runOutput(config, Paths.get(getClass().getClassLoader().getResource("sample_with_column.csv").getPath()));
        }
        catch (IOException ioException) {
            fail(ioException.getMessage());
        }
    }

    @Test
    public void testTransactionCreateNonexistsContainer() throws Exception
    {
        // Azure Container can't be created 30 seconds after deletion.
        String container = "non-exists-container-" + System.currentTimeMillis();
        deleteContainerIfExists(container);

        assertEquals(false, isExistsContainer(container));

        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                                  .set("in", inputConfig())
                                  .set("parser", parserConfig(schemaConfig()))
                                  .set("type", "azure_blob_storage")
                                  .set("account_name", AZURE_ACCOUNT_NAME)
                                  .set("account_key", AZURE_ACCOUNT_KEY)
                                  .set("container", container)
                                  .set("path_prefix", "my-prefix")
                                  .set("file_ext", ".csv")
                                  .set("formatter", formatterConfig());

        Schema schema = CONFIG_MAPPER.map(config.getNested("parser"), CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        try {
            TestingEmbulk.RunResult result = embulk.runOutput(config, Paths.get(getClass().getClassLoader().getResource("sample_with_column.csv").getPath()));
            assertEquals(true, isExistsContainer(container));
            deleteContainerIfExists(container);
        }
        catch (IOException ioException) {
            fail(ioException.getMessage());
        }
    }

    @Test
    public void testCleanup()
    {
        PluginTask task = CONFIG_MAPPER.map(config(), PluginTask.class);
        runner.cleanup(task.toTaskSource(), getSchema(), 0, Lists.<TaskReport>newArrayList()); // no errors happens
    }

    @Test(expected = RuntimeException.class)
    public void testCreateAzureClientThrowsConfigException()
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                                  .set("in", inputConfig())
                                  .set("parser", parserConfig(schemaConfig()))
                                  .set("type", "azure_blob_storage")
                                  .set("account_name", "invalid-account-name")
                                  .set("account_key", AZURE_ACCOUNT_KEY)
                                  .set("container", AZURE_CONTAINER)
                                  .set("path_prefix", "my-prefix")
                                  .set("file_ext", ".csv")
                                  .set("formatter", formatterConfig());

        Schema schema = CONFIG_MAPPER.map(config.getNested("parser"), CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        try {
            TestingEmbulk.RunResult result = embulk.runOutput(config, Paths.get(getClass().getClassLoader().getResource("sample_with_column.csv").getPath()));
        }
        catch (IOException ioException) {
            fail(ioException.getMessage());
        }
    }
}
