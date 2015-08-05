package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.client.CrateClient;
import io.crate.shade.org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class CrateDbClient extends DB {

    public static final String HOSTS_PROPERTY  = "crate.hosts";
    public static final String SHARDS_PROPERTY  = "crate.shards";
    public static final String TABLE_NAME_PROPERTY = "crate.table.name";
    public static final String PRIMARY_KEY_PROPERTY = "crate.table.pk";
    public static final String REPLICAS_PROPERTY = "crate.replicas";

    public static final int OK = 0;
    public static final int FAILURE = 1;

    public static final String DEFAULT_HOST = "localhost:19301";
    public static final String DEFAULT_NUMBER_OF_REPLICAS = "1";
    public static final String DEFAULT_NUMBER_OF_SHARDS = "32";

    public static final String DEFAULT_TABLE_NAME = "usertable";
    public static final String DEFAULT_PRIMARY_KEY = "ycsb_key";
    private static final String DYN_FIELD_NAME = "fields" ;

    private static boolean DEBUG = false;

    protected CrateClient crateClient;
    private String primaryKey;
    private final static Object lock = new Object();

    public void init() throws DBException {
        synchronized (lock) {
            Properties properties = getProperties();
            String[] hosts = properties.getProperty(HOSTS_PROPERTY, DEFAULT_HOST).split(",");
            String tableName = properties.getProperty(TABLE_NAME_PROPERTY, DEFAULT_TABLE_NAME);
            primaryKey = properties.getProperty(PRIMARY_KEY_PROPERTY, DEFAULT_PRIMARY_KEY);
            crateClient = new CrateClient(hosts);
            try {
                createTableIfNotExist(properties, tableName);
            } catch (Exception e) {
                throw new DBException("Could not initialize CrateClient", e);
            }
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        try {
            String stmt = "select fields from " + table + " where " + primaryKey + "=?";
            SQLResponse response = crateClient.sql(new SQLRequest(stmt, new Object[]{key})).actionGet();
            HashMap<String, String> entries = (HashMap) response.rows()[0][0];
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                result.put(entry.getKey(), new StringByteIterator(entry.getValue().toString()));
            }
            return OK;
        } catch (Exception e) {
            System.out.println(String.format("Could not read values for key: %s err: %s",
                    key, e.getLocalizedMessage()));
            return FAILURE;
        }
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        System.out.println("Crate does not support scan");
        return OK;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        try {
            crateClient.sql(new SQLRequest("delete from " + table + " where " + primaryKey + "=?",
                    new Object[]{key})).actionGet();
            return OK;
        } catch (Exception e) {
            System.out.println(String.format("Could not delete value for key: %s err: %s",
                    key, e.getLocalizedMessage()));
            return FAILURE;
        }
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            int idx = 0, len = values.size();
            StringBuilder fieldsBuilder = new StringBuilder();
            Object[] args = new Object[len+1];
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                fieldsBuilder.append(DYN_FIELD_NAME + "['" + entry.getKey() + "']" + " = ?");
                if (idx < len-1) {
                    fieldsBuilder.append(", ");
                }
                args[idx++] = entry.getValue().toString();
            }
            args[idx] = key;
            String stmt = "update " + table + " set " + fieldsBuilder.toString() + " where " + primaryKey + " = ?";
            crateClient.sql(new SQLRequest(stmt, args)).actionGet();
            return OK;
        } catch (Exception e) {
            System.out.println(String.format("Could not update table values for key: %s err: %s",
                    key, e.getLocalizedMessage()));
            return FAILURE;
        }
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            Map<String, String> fieldValues = new HashMap<String, String>();
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                fieldValues.put(entry.getKey(), entry.getValue().toString());
            }
            crateClient.sql(new SQLRequest(prepareInsertStatement(table), new Object[]{key, fieldValues})).actionGet();
            return OK;
        } catch (Exception e) {
            System.out.println(String.format("Could not insert values into table for key: %s err: %s",
                    key, e.getLocalizedMessage()));
            return FAILURE;
        }
    }

    public void dropTable(String table) {
        crateClient.sql("drop table if exists " + table).actionGet();
    }

    public void createTableIfNotExist(Properties properties, String tableName) {
        int shards = Integer.parseInt(properties.getProperty(SHARDS_PROPERTY,
                DEFAULT_NUMBER_OF_SHARDS));
        String replicas = properties.getProperty(REPLICAS_PROPERTY, DEFAULT_NUMBER_OF_REPLICAS);

        StringBuilder fields = new StringBuilder();
        String columnStr = "";
        for (int i = 0; i < 10; i++) {
            columnStr = "field" + String.valueOf(i) + " string";
            if (i > 0) {
                fields.append(", " + columnStr);
            } else {
                fields.append(columnStr);
            }
        }
        StringBuilder stmt = new StringBuilder("create table if not exists  ")
            .append(tableName)
            .append(" ( ").append(primaryKey)
            .append(" string primary key, " +
                    "fields object(dynamic) as (" +
                    fields + ")" +
                    ") " +
                    "clustered into ? shards with (number_of_replicas=?, refresh_interval=0)");
        crateClient.sql(new SQLRequest(stmt.toString(), new Object[]{shards, replicas})).actionGet();
    }

    private String prepareInsertStatement(String table) {
        StringBuilder stmt = new StringBuilder("insert into ")
                .append(table).append(" (")
                .append(primaryKey + ", fields) values (?, ?)");
        return stmt.toString();
    }

    @Override
    public void cleanup() throws DBException {
        crateClient.close();
    }

}
