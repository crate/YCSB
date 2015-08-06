package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import io.crate.shade.com.google.common.base.Joiner;
import io.crate.shade.com.google.common.base.Throwables;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.testng.AssertJUnit.assertEquals;

public class CrateDbClientTest {

    private final static CrateDbClient instance = new CrateDbClient();
    private final static String MOCK_TABLE = "usertable";
    private final static String MOCK_KEY0 = "0";
    private final static String MOCK_KEY1 = "1";
    private final static String MOCK_KEY2 = "2";
    private final static int mockDataSize = 10;
    private final static int dataSize = 100;
    public static final String CRATE_VERSION = "0.50.2";

    private HashMap<String, ByteIterator> mockValues;
    private HashMap<String, ByteIterator> mockValues1;

    private static File workingDirectory;
    private static Task task;

    private final static String CDN_URL = "https://cdn.crate.io/downloads/releases";
    public static List<URI> uris = Arrays.asList(URI.create(String.format("%s/crate-%s.tar.gz", CDN_URL, CRATE_VERSION)));

    @BeforeClass
    public static void setUpClass() throws DBException {
        startCrate();
        instance.init();
    }

    @BeforeMethod
    public void setUp() {
        mockValues = new HashMap<String, ByteIterator>(mockDataSize);
        mockValues1 = new HashMap<String, ByteIterator>(mockDataSize);
        for (int i = 0; i < mockDataSize; i++) {
            mockValues.put("field" + i, new StringByteIterator(genString(dataSize)));
            mockValues1.put("field" + i,  new StringByteIterator(genString(dataSize)));
        }
        instance.insert(MOCK_TABLE, MOCK_KEY1, mockValues);
        instance.insert(MOCK_TABLE, MOCK_KEY2, mockValues1);
        instance.crateClient.sql("refresh table usertable").actionGet();
    }

    private String genString(int dataSize) {
        byte[] array = new byte[dataSize];
        new Random().nextBytes(array);
        return new String(array);
    }

    @Test
    public void testInsert() {
        assertEquals(0, instance.insert(MOCK_TABLE, MOCK_KEY0, mockValues));
    }

    @Test
    public void testDelete() {
        assertEquals(0, instance.delete(MOCK_TABLE, MOCK_KEY1));
    }

    @Test
    public void testRead() {
        Set<String> fields = mockValues.keySet();
        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>(mockValues.size());
        int errCode = instance.read(MOCK_TABLE, MOCK_KEY1, fields, result);
        for (Map.Entry entry : mockValues.entrySet()) {
            assertEquals(entry.getValue().toString(), result.get(entry.getKey()).toString());
        }
        assertEquals(0, errCode);
    }

    @Test
    public void testReadAllFields() {
        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>(mockValues.size());
        assertEquals(0, instance.read(MOCK_TABLE, MOCK_KEY1, null, result));
        for (Map.Entry entry : mockValues.entrySet()) {
            assertEquals(entry.getValue().toString(), result.get(entry.getKey()).toString());
        }
    }

    @Test
    public void testUpdate() {
        assertEquals(0, instance.update(MOCK_TABLE, MOCK_KEY1, mockValues1));
        instance.crateClient.sql("refresh table usertable").actionGet();

        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>(mockValues.size());
        instance.read(MOCK_TABLE, MOCK_KEY1, mockValues.keySet(), result);

        for (String key : result.keySet()) {
            assertEquals(mockValues1.get(key).toString(), result.get(key).toString());
        }
    }

    @AfterMethod
    public void tearDown() {
        instance.delete(MOCK_TABLE, MOCK_KEY0);
        instance.delete(MOCK_TABLE, MOCK_KEY1);
        instance.delete(MOCK_TABLE, MOCK_KEY2);
    }

    @AfterClass
    public static void tearDownClass() throws DBException, IOException {
        instance.dropTable(MOCK_TABLE);
        instance.cleanup();
        stopCrate();
    }

    private static File getOrCreateDataDir() {
        File dataDir = new File("crate.tmp").getAbsoluteFile().getParentFile();
        if (!dataDir.exists()) {
            if (!dataDir.mkdirs()) {
                System.exit(2);
            }
        }
        return dataDir;
    }

    private static boolean prepare() {
        workingDirectory = getOrCreateDataDir();
        boolean success = true;
        for (URI uri : uris) {
            if (!fetchAndExtractUri(uri)) {
                break;
            }
        }
        return success;
    }

    private static boolean fetchAndExtractUri(URI uri) {
        boolean success;
        try {
            URL download = uri.toURL();
            String fn = new File(download.getFile()).getName();
            File tmpFile = new File(fn);
            if (!tmpFile.exists()) {
                System.out.println("Downloading crate tarball...");
                tmpFile.createNewFile();
                ReadableByteChannel rbc = Channels.newChannel(download.openStream());
                FileOutputStream stream = new FileOutputStream(tmpFile);
                stream.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            }
            success = extractFile(tmpFile);
        } catch (IOException e) {
            success = false;
        }
        return success;
    }

    private static boolean extractFile(File tmpFile) {
        System.out.println("Extracting Crate from tarball..");
        try {
            Process process = Runtime.getRuntime().exec(
                    new String[]{
                            "tar",
                            "-C", workingDirectory.getAbsolutePath(),
                            "-xf", tmpFile.getAbsolutePath()
                    },
                    new String[]{},
                    workingDirectory
            );
            process.waitFor();
        } catch (Exception e) {
            Throwables.propagate(e);
        }
        return true;
    }

    public static void startCrate() {
        if (task != null) {
            return;
        }
        task = new Task();
        if (prepare() && task.process == null) {
            try {
                task.run();
                Thread.sleep(13000);
                task.process.exitValue();
            } catch (Exception e) {
            }
            return;
        }
    }

    public static void stopCrate() throws IOException {
        task.gracefulStop();
        Runtime.getRuntime().exec(
                new String[]{"sh", "-c", "rm -rf crate-*"},
                new String[]{},
                workingDirectory
        );
    }

    public static class Task {

        public Process process = null;

        private String cmd() {
            return Joiner.on(" ").join(cmdArgs());
        }

        public  List<String> cmdArgs() {
            List<String> args = new ArrayList<String>(asList(
                    "crate-*/bin/crate",
                    "-p",
                    "crate.pid",
                    String.format("-Des.cluster.name=%s", "testcluster"),
                    String.format("-Des.http.port=%d", Constants.HTTP_PORT),
                    String.format("-Des.transport.tcp.port=%d", Constants.TRANSPORT_PORT),
                    String.format("-Des.discovery.zen.ping.multicast.enabled=%s", "false"),
                    String.format("-Des.gateway.type=%s", "none"),
                    String.format("-Des.index.store.type=%s", "memory"),
                    String.format("-Des.cluster.routing.schedule=%s", "30ms")
            ));
            return args;
        }
        public Process run() throws IOException {
            System.out.println("Starting Crate instance...");
            final String runCmd = String.format("%s", cmd());
            process = Runtime.getRuntime().exec(
                    new String[]{"sh", "-c", runCmd},
                    new String[]{},
                    workingDirectory
            );
            return process;
        }

        public int pid() {
            try {
                FileInputStream pidFile = new FileInputStream("crate.pid");
                BufferedReader in = new BufferedReader(new InputStreamReader(pidFile));
                return Integer.valueOf(in.readLine());
            } catch (IOException e) {
            }
            return -1;
        }

        class GracefulShutdownWorker implements Runnable {
            private final Process process;
            public int exitCode = -1;
            public GracefulShutdownWorker(Process process) {
                this.process = process;
            }
            @Override
            public void run() {
                try {
                    exitCode = this.process.waitFor();
                } catch (InterruptedException e) {
                    exitCode = -1;
                }
            }
        }

        public boolean gracefulStop() {
            int pid = pid();
            boolean success = true;
            try {
                GracefulShutdownWorker worker = new GracefulShutdownWorker(process);
                Thread shutdown = new Thread(worker);
                shutdown.start();
                Runtime.getRuntime().exec(new String[]{"kill", "-USR2", Integer.toString(pid)});
                // todo: set timeout correctly
                shutdown.join(7200000L);
                if (worker.exitCode == -1) {
                    throw new InterruptedIOException();
                }
            } catch (Exception e) {
                success = false;
            }
            return success;
        }
    }

}


