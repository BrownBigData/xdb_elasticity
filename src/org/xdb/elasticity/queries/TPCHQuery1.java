package org.xdb.elasticity.queries;

import java.util.Map;

import org.xdb.elasticity.DistributedQuery;
import org.xdb.elasticity.PartitionTable;

public class TPCHQuery1 extends DistributedQuery {

	public TPCHQuery1() {
		super(Q1);
	}

	@Override
	public void updatePartitionTable(Map<String, PartitionTable> catalog) {
		setPartitionTable(catalog.get("lineitem"));
	}
	
	private static  String Q1 = "select l_returnflag, l_linestatus, "
			+ "sum(l_quantity) as sum_qty, "
			+ "sum(l_extendedprice) as sum_base_price, "
			+ "sum(l_extendedprice*(1-l_discount)) as sum_disc_price, "
			+ "sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, "
			+ "avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, "
			+ "avg(l_discount) as avg_disc, count(*) as count_order "
			+ "from lineitem_<P> "
			+ "where l_shipdate <= date '1998-11-28' "
			+ "group by l_returnflag, l_linestatus "
			+ "order by l_returnflag, l_linestatus;";
}
