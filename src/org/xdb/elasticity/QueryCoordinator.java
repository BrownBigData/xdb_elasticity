package org.xdb.elasticity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import org.xdb.Config;

public class QueryCoordinator extends Thread {
	public class Coordinator extends Thread {
		public Coordinator() {

		}
		
		public boolean running(){
			return m_running;
		}

		@Override
		public void run() {
			
			try {
				m_serverSocket = new ServerSocket(Config.COORDINATOR_PORT);
				m_running = true;
				
				while (true) {
					Socket clientSocket = m_serverSocket.accept();
					BufferedReader in = new BufferedReader(
							new InputStreamReader(clientSocket.getInputStream()));
					
					String line = in.readLine();
					String[] catalogEntry = line.split(",");
					String table = catalogEntry[0];
					String part = catalogEntry[1];
					String url = catalogEntry[2];
					Connection conn = DriverManager.getConnection(url,
							Config.DB_USER, Config.DB_PASSWD);
				
					QueryCoordinator.this.updateCatalog(table, part, conn);
				}

			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
		}

		private ServerSocket m_serverSocket;
		private boolean m_running = false;
	}

	public QueryCoordinator(int clientCount) {
		m_catalog = new HashMap<String, PartitionTable>();
		m_clientCount = clientCount;

		try {
			Class.forName(Config.JDBC_DRIVER);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

		loadCatalog();
	}

	public synchronized Map<String, PartitionTable> getCatalog() {
		return m_catalog;
	}

	public synchronized void updateCatalog(String table, String partition,
			Connection conn) {
		PartitionTable partitionTable = m_catalog.get(table);

		if (partitionTable == null) {
			partitionTable = new PartitionTable();
			m_catalog.put(table, partitionTable);
		}

		partitionTable.update(partition, conn);
	}

	public void loadCatalog() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					Config.CATALOG_FILE));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] catalogEntry = line.split(",");
				String table = catalogEntry[0];
				String part = catalogEntry[1];
				String url = "jdbc:mysql://"+catalogEntry[2]+"/"+Config.DB_NAME+"?useSSL=false";
				Connection conn = DriverManager.getConnection(url,
						Config.DB_USER, Config.DB_PASSWD);
				updateCatalog(table, part, conn);
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	@Override
	public void run() {
		// start coordinator
		Coordinator coord = new Coordinator();
		coord.start();
		while(!coord.running()){
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(0);
			}
		}
		
		// start clients
		for (int i = 0; i < m_clientCount; ++i) {
			QueryClient client = new QueryClient(this);
			client.start();
		}

		// run in loop and report throughput
		System.out.println("Runtime, \t Queries");
		System.out.println("-------------------------");

		m_timestamp = System.currentTimeMillis();
		while (true) {
			try {
				Thread.sleep(Config.TIMER_INTERVAL);

				long now = System.currentTimeMillis();
				long time = now - m_timestamp;
				System.out.println(time + ",\t " + m_queryCount);

				m_queryCount = 0;
				m_timestamp = now;

			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
		}
	}

	public synchronized void incrementCount() {
		m_queryCount++;
	}

	private Map<String, PartitionTable> m_catalog;
	private long m_queryCount = 0;
	private int m_clientCount = 0;
	private long m_timestamp = 0;
}
