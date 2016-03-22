package org.xdb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class InstallRemote {
	public InstallRemote(String url){
		m_url = url;
	}
	
	public void run(){
		try {
			// connect to local DB
			Class.forName(Config.JDBC_DRIVER);
			String jdbcUrl = "jdbc:mysql://"+m_url+"/tpch_s01?useSSL=false";
			Connection conn = DriverManager.getConnection(jdbcUrl,
					Config.DB_USER, Config.DB_PASSWD);

			// read remote file
			Map<String, String> remoteTableDDLs = new HashMap<String, String>();
			BufferedReader reader = new BufferedReader(new FileReader(
					Config.REMOTE_FILE));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] catalogEntry = line.split(";");
				String table = catalogEntry[0];
				String ddl = catalogEntry[1];
				remoteTableDDLs.put(table, ddl);
			}
			reader.close();

			//create remote schema
			reader = new BufferedReader(new FileReader(
					Config.MIGRATE_FILE));
			line = null;
			while ((line = reader.readLine()) != null) {
				String[] catalogEntry = line.split(",");
				String table = catalogEntry[0];
				String part = catalogEntry[1];
				String host = catalogEntry[2];
				String ddl = remoteTableDDLs.get(table);
				
				ddl = ddl.replaceAll("<P>", part);
				ddl = ddl.replaceAll("<H>", host);
				
				Statement stmt = conn.createStatement();
				System.out.println("DDL: "+ddl);
				stmt.execute(ddl);
			}
			reader.close();
			
			
			// close DB conn
			conn.close();

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private String m_url;
	
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Usage: InstallRemote <url>");
			return;
		}
		System.out.println("Schema installation started ...");
		InstallRemote client = new InstallRemote(args[0]);
		client.run();
		System.out.println("Finished!");
	}
}
