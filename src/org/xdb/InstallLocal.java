package org.xdb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class InstallLocal {
	public InstallLocal(String url){
		m_url = url;
	}
	
	public void run(){
		try {
			// connect to local DB
			Class.forName(Config.JDBC_DRIVER);
			String jdbcUrl = "jdbc:mysql://"+m_url+"/"+Config.DB_NAME+"?useSSL=false";
			Connection conn = DriverManager.getConnection(jdbcUrl,
					Config.DB_USER, Config.DB_PASSWD);

			// read local file
			Map<String, String> remoteTableDDLs = new HashMap<String, String>();
			BufferedReader reader = new BufferedReader(new FileReader(
					Config.LOCAL_FILE));
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
					Config.CATALOG_FILE));
			line = null;
			while ((line = reader.readLine()) != null) {
				String[] catalogEntry = line.split(",");
				String table = catalogEntry[0];
				String part = catalogEntry[1];
				String ddl = remoteTableDDLs.get(table);
				
				ddl = ddl.replaceAll("<P>", part);
				ddl = ddl.replaceAll("<D>", Config.DB_NAME);
				
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
		if (args.length != 2) {
			System.out.println("Usage: InstallLocal <dbname> <hostname>");
			return;
		}
		
		Config.DB_NAME = args[0];
		String hostname = args[1];
		
		System.out.println("Schema installation started ...");
		InstallLocal client = new InstallLocal(hostname);
		client.run();
		System.out.println("Finished!");
	}
}
