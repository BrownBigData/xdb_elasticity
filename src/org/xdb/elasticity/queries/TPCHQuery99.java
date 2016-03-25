package org.xdb.elasticity.queries;

import java.util.Map;

import org.xdb.elasticity.DistributedQuery;
import org.xdb.elasticity.PartitionTable;

public class TPCHQuery99 extends DistributedQuery {

	public TPCHQuery99() {
		super(Q99);
	}

	@Override
	public void updatePartitionTable(Map<String, PartitionTable> catalog) {
		try {
			PartitionTable oldTable = this.getPartitionTable();
			PartitionTable newTable = new PartitionTable();
			PartitionTable lineitemParts = catalog.get("lineitem");
			PartitionTable ordersParts = catalog.get("orders");
			PartitionTable customerParts = catalog.get("customer");
			PartitionTable suppParts = catalog.get("supplier");
			PartitionTable partParts = catalog.get("part");
			PartitionTable nationParts = catalog.get("nation");
			PartitionTable regionParts = catalog.get("region");

			for (String partition : lineitemParts.partitions()) {
				boolean update = true;
				
				String lineitemConn = lineitemParts.getConnection(partition)
						.getMetaData().getURL();
				String ordersConn = ordersParts.getConnection(partition)
						.getMetaData().getURL();
				String customerConn = customerParts.getConnection(partition)
						.getMetaData().getURL();
				String suppConn = suppParts.getConnection(partition)
						.getMetaData().getURL();
				String partConn = partParts.getConnection(partition)
						.getMetaData().getURL();
				String nationConn = nationParts.getConnection(partition)
						.getMetaData().getURL();
				String regionConn = regionParts.getConnection(partition)
						.getMetaData().getURL();
				
				if (!lineitemConn.equals(ordersConn)) {
					update = false;
				} else if (!lineitemConn.equals(customerConn)) {
					update = false;
				} else if (!lineitemConn.equals(suppConn)) {
					update = false;
				} else if (!lineitemConn.equals(partConn)) {
					update = false;
				}
				else if (!lineitemConn.equals(nationConn)) {
					update = false;
				} else if (!lineitemConn.equals(regionConn)) {
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

	private static String Q99 = "select "
			+ "p_name, "
			+ "n_name, "
			+ "sum(l_extendedprice * (1-l_discount)) as revenue "
			+ "from  customer_<P>, orders_<P>, "
			+ "lineitem_<P>, supplier_<P>, part_<P>, "
			+ "nation_<P>, region_<P> "
			+ "where c_custkey = o_custkey "
			+ "and l_orderkey = o_orderkey " 
			+ "and l_partkey = p_partkey " 
			+ "and l_suppkey = s_suppkey "
			+ "and c_nationkey = s_nationkey "
			+ "and s_nationkey = n_nationkey "
			+ "and r_regionkey = n_regionkey " + "and r_name = 'ASIA' "
			+ "and o_orderdate > DATE('1994-01-01')  "
			+ "and o_orderdate < DATE('1995-01-01') " + "group by p_name, n_name;";
}
