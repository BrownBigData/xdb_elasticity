package org.xdb;

public class Config {
	public static final String REMOTE_FILE = "./conf/remote.conf";
	public static final String LOCAL_FILE = "./conf/local.conf";
	public static final String LOAD_FILE = "./conf/load.conf";
	public static final String MIGRATE_FILE = "./conf/migration.conf";
	public static final String CATALOG_FILE = "./conf/catalog.conf";
	public static final String DB_USER = "xroot";
	public static final String DB_PASSWD = "xroot";
	public static String DB_NAME = "tpch_s01";
	public static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	public static final int RAND_SEED = 0;
	public static final int TIMER_INTERVAL = 1000*60;
	public static final int COORDINATOR_PORT = 4000;
}
