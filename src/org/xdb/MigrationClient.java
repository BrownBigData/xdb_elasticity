package org.xdb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MigrationClient {
	public MigrationClient(String url) {
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

			// apply migration file
			BufferedReader reader = new BufferedReader(new FileReader(
					Config.MIGRATE_FILE));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] catalogEntry = line.split(",");
				String table = catalogEntry[0];
				String part = catalogEntry[1];
				copyTable(table, part, conn);
				notifyCoordinator(table, part);
			}
			reader.close();

			// close DB conn
			conn.close();

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	private void notifyCoordinator(String table, String part) {
		try {
			Socket sock = new Socket(Config.COORDINATOR_URL,
					Config.COORDINATOR_PORT);
			PrintWriter out = new PrintWriter(sock.getOutputStream(), true);

			String url = URL_TEMPLATE.replaceAll("<H>", m_url);
			String msg = table + "," + part + "," + url;

			System.out.println("NOTIFY: " + msg);

			out.write(msg);
			out.flush();

			sock.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	private void copyTable(String table, String part, Connection conn) {
		String copyQuery = COPY_QUERY.replaceAll("<T>", table);
		copyQuery = copyQuery.replaceAll("<P>", part);
		try {
			System.out.println("COPY: " + copyQuery);

			Statement stmt = conn.createStatement();
			stmt.execute(copyQuery);
			System.out.println(copyQuery);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	private String m_url;

	private static String COPY_QUERY = "INSERT INTO <T>_<P> SELECT * FROM <T>_<P>_remote";
	private static String URL_TEMPLATE = "jdbc:mysql://<H>/" + Config.DB_NAME
			+ "?useSSL=false";

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Usage: MigrationClient <dbname> <hostname>");
			return;
		}

		// change config
		Config.DB_NAME = args[0];
		String hostname = args[1];

		System.out.println("Migration started ...");
		MigrationClient client = new MigrationClient(hostname);
		client.run();
		System.out.println("Finished!");
	}

}
