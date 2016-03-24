package org.xdb.elasticity.queries;

import java.util.Map;

import org.xdb.elasticity.DistributedQuery;
import org.xdb.elasticity.PartitionTable;

public class TPCHQuery3 extends DistributedQuery {

	public TPCHQuery3() {
		super(Q3);
	}

	@Override
	public void updatePartitionTable(Map<String, PartitionTable> catalog) {
		try {
			PartitionTable oldTable = this.getPartitionTable();
			PartitionTable newTable = new PartitionTable();
			PartitionTable lineitemParts = catalog.get("lineitem");
			PartitionTable ordersParts = catalog.get("orders");
			PartitionTable customerParts = catalog.get("customer");

			System.out.println(catalog.keySet());
			
			for (String partition : lineitemParts.partitions()) {
				boolean update = true;
				
				String lineitemConn = lineitemParts.getConnection(partition)
						.getMetaData().getURL();
				String ordersConn = ordersParts.getConnection(partition)
						.getMetaData().getURL();
				String customerConn = customerParts.getConnection(partition)
						.getMetaData().getURL();
				
				if (!lineitemConn.equals(ordersConn)) {
					update = false;
					break;
				} else if (!lineitemConn.equals(customerConn)) {
					update = false;
					break;
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

	private static String Q3 = "select l_orderkey, "
			+ "sum(l_extendedprice*(1-l_discount)) as revenue, "
			+ "o_orderdate, " + "o_shippriority " + "from "
			+ "customer_<P>, orders_<P>, lineitem_<P> "
			+ "where c_mktsegment = 'BUILDING' and "
			+ "c_custkey = o_custkey and " + "l_orderkey = o_orderkey and "
			+ "o_orderdate < date '1995-03-15' and "
			+ "l_shipdate > date '1995-03-15' "
			+ "group by l_orderkey, o_orderdate, o_shippriority "
			+ "order by revenue desc, o_orderdate " + "limit 10;";
}
