package org.embulk.output.azure_blob_storage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.List;

import static org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.CONFIG_MAPPER_FACTORY;

public class TestHelper
{
    public static String AZURE_ACCOUNT_NAME;
    public static String AZURE_ACCOUNT_KEY;
    public static String AZURE_CONTAINER;
    public static String AZURE_CONTAINER_DIRECTORY;
    public static String LOCAL_PATH_PREFIX;
    public static String AZURE_PATH_PREFIX;

    /*
     * This test case requires environment variables
     *   AZURE_ACCOUNT_NAME
     *   AZURE_ACCOUNT_KEY
     *   AZURE_CONTAINER
     *   AZURE_CONTAINER_DIRECTORY
     */
    static {
        AZURE_ACCOUNT_NAME = getenv("AZURE_ACCOUNT_NAME");
        AZURE_ACCOUNT_KEY = getenv("AZURE_ACCOUNT_KEY");
        AZURE_CONTAINER = getenv("AZURE_CONTAINER");
        // skip test cases, if environment variables are not set.
        AZURE_CONTAINER_DIRECTORY = System.getenv("AZURE_CONTAINER_DIRECTORY") != null ? getDirectory(System.getenv("AZURE_CONTAINER_DIRECTORY")) : getDirectory("");
        AZURE_PATH_PREFIX = AZURE_CONTAINER_DIRECTORY + "sample_";
        LOCAL_PATH_PREFIX = Resources.getResource("sample_01.csv")
                .getPath();
    }

    private TestHelper()
    {
    }

    // GitHub Actions passes a secret that is not set as an empty string, so treat it as not set and skip the tests
    private static String getenv(String name)
    {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? null : value;
    }

    public static byte[] convertInputStreamToByte(InputStream is) throws IOException
    {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        while (true) {
            int len = is.read(buffer);
            if (len < 0) {
                break;
            }
            bo.write(buffer, 0, len);
        }
        return bo.toByteArray();
    }

    public static ImmutableList<List<String>> getFileContentsFromAzure(String path) throws Exception
    {
        Method method = AzureBlobStorageFileOutputPlugin.class.getDeclaredMethod("newAzureClient", String.class, String.class);
        method.setAccessible(true);
        CloudBlobClient client = newAzureClient(AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY);
        CloudBlobContainer container = client.getContainerReference(AZURE_CONTAINER);
        CloudBlob blob = container.getBlockBlobReference(path);

        ImmutableList.Builder<List<String>> builder = new ImmutableList.Builder<>();

        InputStream is = blob.openInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line;
        while ((line = reader.readLine()) != null) {
            List<String> records = Arrays.asList(line.split(",", 0));

            builder.add(records);
        }
        return builder.build();
    }

    public static CloudBlobClient newAzureClient(String accountName, String accountKey)
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

    public static String getDirectory(String dir)
    {
        if (!dir.isEmpty() && !dir.endsWith("/")) {
            dir = dir + "/";
        }
        if (dir.startsWith("/")) {
            dir = dir.replaceFirst("/", "");
        }
        return dir;
    }

    public static void deleteContainerIfExists(String containerName) throws Exception
    {
        Method method = AzureBlobStorageFileOutputPlugin.class.getDeclaredMethod("newAzureClient", String.class, String.class);
        method.setAccessible(true);
        CloudBlobClient client = newAzureClient(AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY);
        CloudBlobContainer container = client.getContainerReference(containerName);
        if (container.exists()) {
            container.delete();
            // container could not create same name after deletion.
            Thread.sleep(30000);
        }
    }

    public static boolean isExistsContainer(String containerName) throws Exception
    {
        Method method = AzureBlobStorageFileOutputPlugin.class.getDeclaredMethod("newAzureClient", String.class, String.class);
        method.setAccessible(true);
        CloudBlobClient client = newAzureClient(AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY);
        CloudBlobContainer container = client.getContainerReference(containerName);
        return container.exists();
    }

    public static ConfigSource config()
    {
        return CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "azure_blob_storage")
                .set("account_name", AZURE_ACCOUNT_NAME)
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", AZURE_CONTAINER)
                .set("path_prefix", AZURE_PATH_PREFIX)
                .set("last_path", "")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());
    }

    static ImmutableMap<String, Object> inputConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", LOCAL_PATH_PREFIX);
        builder.put("last_path", "");
        return builder.build();
    }

    static ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig)
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 1);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }

    static ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "account", "type", "long"));
        builder.add(ImmutableMap.of("name", "time", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "purchase", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "comment", "type", "string"));
        builder.add(ImmutableMap.of("name", "json_column", "type", "json"));
        return builder.build();
    }

    static Schema getSchema()
    {
        Schema.Builder builder = new Schema.Builder();
        builder.add("id", Types.LONG);
        builder.add("account", Types.LONG);
        builder.add("time", Types.TIMESTAMP);
        builder.add("purchase", Types.TIMESTAMP);
        builder.add("comment", Types.STRING);
        builder.add("name", Types.JSON);
        return builder.build();
    }

    static ImmutableMap<String, Object> formatterConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("header_line", "false");
        builder.put("timezone", "Asia/Tokyo");
        return builder.build();
    }

    public static class Control implements OutputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource)
        {
            return Lists.newArrayList(CONFIG_MAPPER_FACTORY.newTaskReport());
        }
    }
}
