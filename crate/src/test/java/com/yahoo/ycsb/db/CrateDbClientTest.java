package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Set;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class CrateDbClientTest {

    private final static CrateDbClient instance = new CrateDbClient();
    private final static String MOCK_TABLE = "usertable";
    private final static String MOCK_KEY0 = "0";
    private final static String MOCK_KEY1 = "1";
    private final static String MOCK_KEY2 = "2";
    private final static HashMap<String, ByteIterator> mockValues;
    private final static int mockDataSize = 10;

    static {
        mockValues = new HashMap<String, ByteIterator>(mockDataSize);
        for (int i = 0; i < mockDataSize; i++) {
            mockValues.put("field" + i, new StringByteIterator("value" + i));
        }
    }

    @BeforeClass
    public static void setUpClass() throws DBException {
        instance.init();
    }

    @BeforeMethod
    public void setUp() {
        instance.insert(MOCK_TABLE, MOCK_KEY1, mockValues);
        instance.insert(MOCK_TABLE, MOCK_KEY2, mockValues);
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
        assertTrue(fields.equals(result.keySet()));
        assertEquals(0, errCode);
    }

    @Test
    public void testReadAllFields() {
        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>(mockValues.size());
        assertEquals(0, instance.read(MOCK_TABLE, MOCK_KEY1, null, result));
        assertTrue(mockValues.keySet().equals(result.keySet()));
    }

    @Test
    public void testUpdate() {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>(mockValues.size());
        for (String key : mockValues.keySet()) {
            values.put(key, new StringByteIterator("updated" + key));
        }
        assertEquals(0, instance.update(MOCK_TABLE, MOCK_KEY1, values));

        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>(mockValues.size());
        instance.read(MOCK_TABLE, MOCK_KEY1, mockValues.keySet(), result);

        for (String key : result.keySet()) {
            assertEquals(values.get(key).toString(), result.get(key).toString());
        }
    }

    @AfterMethod
    public void tearDown() {
        instance.delete(MOCK_TABLE, MOCK_KEY0);
        instance.delete(MOCK_TABLE, MOCK_KEY1);
        instance.delete(MOCK_TABLE, MOCK_KEY2);
    }

    @AfterClass
    public static void tearDownClass() throws DBException {
        instance.dropTable(MOCK_TABLE);
        instance.cleanup();
    }

}
