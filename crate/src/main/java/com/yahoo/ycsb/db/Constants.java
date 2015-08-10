package com.yahoo.ycsb.db;

import java.util.Random;

public interface Constants {

    final Random rand = new Random();

    public static final int HTTP_PORT = 44200 + rand.nextInt(100);
    public static final int TRANSPORT_PORT = 44300 + rand.nextInt(100);

    public static final String HOSTS_PROPERTY  = "crate.hosts";
    public static final String SHARDS_PROPERTY  = "crate.shards";
    public static final String TABLE_NAME_PROPERTY = "crate.table.name";
    public static final String PRIMARY_KEY_PROPERTY = "crate.table.pk";
    public static final String REPLICAS_PROPERTY = "crate.replicas";

    public static final String DEFAULT_HOST = String.format("localhost:%s", TRANSPORT_PORT);
    public static final String DEFAULT_NUMBER_OF_REPLICAS = "1";
    public static final String DEFAULT_NUMBER_OF_SHARDS = "32";


    public static final String DEFAULT_TABLE_NAME = "usertable";
    public static final String DEFAULT_PRIMARY_KEY = "ycsb_key";

}
