package org.xdb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class InstallRemote {
	public InstallRemote(String url) {
		m_url = url;
	}

	public void run() {
		try {
			// connect to local DB
			Class.forName(Config.JDBC_DRIVER);
			String jdbcUrl = "jdbc:mysql://" + m_url + "/" + Config.DB_NAME
					+ "?useSSL=false";
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

			// read local file
			Map<String, String> localTableDDLs = new HashMap<String, String>();
			reader = new BufferedReader(new FileReader(Config.LOCAL_FILE));
			line = null;
			while ((line = reader.readLine()) != null) {
				String[] catalogEntry = line.split(";");
				String table = catalogEntry[0];
				String ddl = catalogEntry[1];
				localTableDDLs.put(table, ddl);
			}
			reader.close();

			// create remote schema
			reader = new BufferedReader(new FileReader(Config.MIGRATE_FILE));
			line = null;
			while ((line = reader.readLine()) != null) {
				String[] catalogEntry = line.split(",");
				String table = catalogEntry[0];
				String part = catalogEntry[1];
				String host = catalogEntry[2];

				String localDDL = localTableDDLs.get(table);
				localDDL = localDDL.replaceAll("<P>", part);
				localDDL = localDDL.replaceAll("<D>", Config.DB_NAME);
				Statement stmt = conn.createStatement();
				System.out.println("LOCAL (" + m_url + "): " + localDDL);
				stmt.execute(localDDL);
				stmt.close();

				if (remoteTableDDLs.containsKey(table)) {
					String remoteDDL = remoteTableDDLs.get(table);
					remoteDDL = remoteDDL.replaceAll("<P>", part);
					remoteDDL = remoteDDL.replaceAll("<H>", host);
					remoteDDL = remoteDDL.replaceAll("<D>", Config.DB_NAME);
					stmt = conn.createStatement();
					System.out.println("REMOTE (" + host + "): " + remoteDDL);
					stmt.execute(remoteDDL);
					stmt.close();
				}
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
			System.out.println("Usage: InstallRemote <dbname> <hostname>");
			return;
		}

		Config.DB_NAME = args[0];
		String hostname = args[1];

		System.out.println("Schema installation started ...");
		InstallRemote client = new InstallRemote(hostname);
		client.run();
		System.out.println("Finished!");
	}
}
