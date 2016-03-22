package org.xdb.elasticity;

import java.sql.Connection;
import java.util.Map;
import java.util.Vector;

public abstract class DistributedQuery {
	public DistributedQuery(String query){
		m_query = query;
		m_partitionTable = new PartitionTable();
	}
	
	public abstract void updatePartitionTable(Map<String, PartitionTable> catalog);
	
	public void run() {
		Vector<QueryThread> queryThreads = new Vector<QueryThread>();

		// start queries on partitions
		PartitionTable pTable = getPartitionTable();
		for (String part : pTable.getPartitions()) {
			Connection conn = pTable.getConnection(part);
			String query = m_query.replaceAll("<P>", part);
			
			QueryThread queryThread = new QueryThread(conn, query);
			queryThread.start();
			queryThreads.add(queryThread);
		}
		
		
		//wait for all queries to finish
		for(QueryThread queryThread: queryThreads){
			try {
				queryThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(0);
			}
		}
	}
	
	protected synchronized void setPartitionTable(PartitionTable partitionTable){
		m_partitionTable = partitionTable;
	}
	
	protected synchronized PartitionTable getPartitionTable(){
		return m_partitionTable;
	}
	
	private String m_query;
	private PartitionTable m_partitionTable;
}
