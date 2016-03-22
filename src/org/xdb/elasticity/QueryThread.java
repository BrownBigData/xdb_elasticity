package org.xdb.elasticity;

import java.sql.Connection;
import java.sql.Statement;

public class QueryThread extends Thread {
	public QueryThread(Connection conn, String query) {
		m_query = query;
		m_finished = false;
		m_conn = conn;
	}

	@Override
	public void run() {
		try {
			Statement stmt = m_conn.createStatement();
			//System.out.println(m_query);
			stmt.execute(m_query);
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		m_finished = true;
	}

	public boolean finished() {
		return m_finished;
	}

	private String m_query;
	private Connection m_conn;
	private boolean m_finished;
}
