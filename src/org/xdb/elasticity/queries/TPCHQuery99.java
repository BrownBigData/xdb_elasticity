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
			PartitionTable ordersParts = catalog.get("orders");
			PartitionTable customerParts = catalog.get("customer");
			PartitionTable nationParts = catalog.get("nation");
			PartitionTable regionParts = catalog.get("region");

			for (String partition : ordersParts.partitions()) {
				boolean update = true;
				
				String ordersConn = ordersParts.getConnection(partition)
						.getMetaData().getURL();
				String customerConn = customerParts.getConnection(partition)
						.getMetaData().getURL();
				String nationConn = nationParts.getConnection(partition)
						.getMetaData().getURL();
				String regionConn = regionParts.getConnection(partition)
						.getMetaData().getURL();
				
				if (!ordersConn.equals(customerConn)) {
					update = false;
				} else if (!ordersConn.equals(nationConn)) {
					update = false;
				} else if (!ordersConn.equals(regionConn)) {
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

	private static String Q99 = "select "
			+ "n_name, "
			+ "count(*) as total "
			+ "from  customer_<P>, orders_<P>, "
			+ "nation_<P>, region_<P> "
			+ "where c_custkey = o_custkey "
			+ "and c_nationkey = n_nationkey "
			+ "and r_regionkey = n_regionkey " 
			+ "and r_name = 'ASIA' "
			+ "group by n_name;";
}
