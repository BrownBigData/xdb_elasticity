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
	public MigrationClient(String host, String coord) {
		m_coord = coord;
		m_conn = "jdbc:mysql://"+host+"/" + Config.DB_NAME
				+ "?useSSL=false";
	}

	public void run() {
		try {
			// connect to local DB
			Class.forName(Config.JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(m_conn,
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
			Socket sock = new Socket(m_coord,
					Config.COORDINATOR_PORT);
			PrintWriter out = new PrintWriter(sock.getOutputStream(), true);

			String msg = table + "," + part + "," + m_conn;

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

	private String m_coord;
	private String m_conn;

	private static String COPY_QUERY = "INSERT INTO <T>_<P> SELECT * FROM <T>_<P>_remote";
	
	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: MigrationClient <dbname> <hostname> <coord>");
			return;
		}

		// change config
		Config.DB_NAME = args[0];
		String hostname = args[1];
		String coord = args[2];

		System.out.println("Migration started ...");
		MigrationClient client = new MigrationClient(hostname, coord);
		client.run();
		System.out.println("Finished!");
	}

}
