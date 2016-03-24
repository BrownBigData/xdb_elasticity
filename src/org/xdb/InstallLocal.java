package org.xdb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class InstallLocal {
	public InstallLocal(String path) {
		m_path = path;
		m_conns = new HashMap<String, Connection>();
	}

	public void run() {
		try {
			// read local file
			Map<String, String> createTableDDLs = new HashMap<String, String>();
			BufferedReader reader = new BufferedReader(new FileReader(
					Config.LOCAL_FILE));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] catalogEntry = line.split(";");
				String table = catalogEntry[0];
				String ddl = catalogEntry[1];
				createTableDDLs.put(table, ddl);
			}
			reader.close();

			// read load file
			Map<String, String> loadTableDDLs = new HashMap<String, String>();
			reader = new BufferedReader(new FileReader(Config.LOAD_FILE));
			line = null;
			while ((line = reader.readLine()) != null) {
				String[] catalogEntry = line.split(";");
				String table = catalogEntry[0];
				String ddl = catalogEntry[1];
				loadTableDDLs.put(table, ddl);
			}
			reader.close();

			// create remote schema
			reader = new BufferedReader(new FileReader(Config.CATALOG_FILE));
			line = null;
			while ((line = reader.readLine()) != null) {
				String[] catalogEntry = line.split(",");
				String table = catalogEntry[0];
				String part = catalogEntry[1];
				String host = catalogEntry[2];

				Connection conn = null;
				if (!m_conns.containsKey(host)) {
					// connect to local DB
					Class.forName(Config.JDBC_DRIVER);
					String jdbcUrl = "jdbc:mysql://" + host + "/"
							+ Config.DB_NAME + "?useSSL=false";
					conn = DriverManager.getConnection(jdbcUrl, Config.DB_USER,
							Config.DB_PASSWD);

				} else {
					conn = m_conns.get(host);
				}

				String createDDL = createTableDDLs.get(table);
				createDDL = createDDL.replaceAll("<P>", part);
				Statement stmt = conn.createStatement();
				System.out.println("CREATE (" + host + "): " + createDDL);
				stmt.execute(createDDL);
				stmt.close();

				if (loadTableDDLs.containsKey(table)) {
					String loadDDL = loadTableDDLs.get(table);
					loadDDL = loadDDL.replaceAll("<P>", part);
					loadDDL = loadDDL.replaceAll("<L>", m_path);
					stmt = conn.createStatement();
					System.out.println("LOAD (" + host + "): " + loadDDL);
					stmt.execute(loadDDL);
					stmt.close();
				}
			}

			reader.close();

			// clean up
			for (Connection conn : m_conns.values()) {
				conn.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	private Map<String, Connection> m_conns;
	private String m_path;

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Usage: InstallLocal <dbname> <path>");
			return;
		}

		Config.DB_NAME = args[0];
		String path = args[1];

		System.out.println("Schema installation started ...");
		InstallLocal client = new InstallLocal(path);
		client.run();
		System.out.println("Finished!");
	}
}
