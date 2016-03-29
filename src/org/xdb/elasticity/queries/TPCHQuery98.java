package org.xdb.elasticity.queries;

import java.util.Map;

import org.xdb.elasticity.DistributedQuery;
import org.xdb.elasticity.PartitionTable;

public class TPCHQuery98 extends DistributedQuery {

	public TPCHQuery98() {
		super(Q98);
	}

	@Override
	public void updatePartitionTable(Map<String, PartitionTable> catalog) {
		try {
			PartitionTable oldTable = this.getPartitionTable();
			PartitionTable newTable = new PartitionTable();
			PartitionTable ordersParts = catalog.get("orders");
			PartitionTable customerParts = catalog.get("customer");

			for (String partition : ordersParts.partitions()) {
				boolean update = true;
				
				String ordersConn = ordersParts.getConnection(partition)
						.getMetaData().getURL();
				String customerConn = customerParts.getConnection(partition)
						.getMetaData().getURL();
				
				if (!ordersConn.equals(customerConn)) {
					update = false;
				}
				
				if(update){
					newTable.update(partition, ordersParts.getConnection(partition));
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

	private static String Q98 = "select c_nationkey, "
			+ "count(*) "
			+ "from "
			+ "customer_<P>, orders_<P> "
			+ "where c_mktsegment = 'BUILDING' and "
			+ "c_custkey = o_custkey and "
			+ "o_orderdate < date '1995-03-15' "
			+ "group by c_nationkey;";
}
