package org.xdb.elasticity;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PartitionTable {
	
	public PartitionTable() {
		m_partitions = new HashMap<String, Connection>();
	}
	
	public Set<String> partitions(){
		return m_partitions.keySet();
	}

	public void update(String partition, Connection conn) {
		m_partitions.put(partition, conn);
	}

	public Map<String, Connection> getTable() {
		return m_partitions;
	}
	
	public Connection getConnection(String partition){
		return m_partitions.get(partition);
	}

	public Set<String> getPartitions(){
		return m_partitions.keySet();
	}
	
	private Map<String, Connection> m_partitions;
}
