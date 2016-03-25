package org.xdb.elasticity.queries;

import java.util.Map;

import org.xdb.elasticity.DistributedQuery;
import org.xdb.elasticity.PartitionTable;

public class TPCHQuery14 extends DistributedQuery {

	public TPCHQuery14() {
		super(Q3);
	}

	@Override
	public void updatePartitionTable(Map<String, PartitionTable> catalog) {
		try {
			PartitionTable oldTable = this.getPartitionTable();
			PartitionTable newTable = new PartitionTable();
			PartitionTable lineitemParts = catalog.get("lineitem");
			PartitionTable partParts = catalog.get("part");

			for (String partition : lineitemParts.partitions()) {
				boolean update = true;
				
				String lineitemConn = lineitemParts.getConnection(partition)
						.getMetaData().getURL();
				String partConn = partParts.getConnection(partition)
						.getMetaData().getURL();
				
				if (!lineitemConn.equals(partConn)) {
					update = false;
				} 
				
				if(update){
					newTable.update(partition, lineitemParts.getConnection(partition));
				}
				else{
					newTable.update(partition, oldTable.getConnection(partition));
				}
			}

			setPartitionTable(newTable);

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	private static String Q3 = 
			"SELECT sum(CASE WHEN p_type LIKE 'PROMO%' "
			+ "THEN l_extendedprice * (1 - l_discount) ELSE "
			+ "0 END) / "
			+ "sum(l_extendedprice * (1 - l_discount)) AS promo_revenue " + 
			"FROM lineitem_<p>, " + 
			"     part_<p> " + 
			"WHERE l_partkey = p_partkey " + 
			"    AND l_shipdate >= date '1995-09-01' " + 
			"    AND l_shipdate < date '1995-10-01';";
}
