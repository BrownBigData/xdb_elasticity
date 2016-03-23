package org.xdb.elasticity;

import java.util.Random;
import java.util.Vector;

import org.xdb.Config;
//import org.xdb.elasticity.queries.TPCHQuery1;
import org.xdb.elasticity.queries.TPCHQuery3;

public class QueryClient extends Thread {
	
	public QueryClient(QueryCoordinator coord) {
		m_coord = coord;
		m_queries = new Vector<DistributedQuery>();
		//m_queries.add(new TPCHQuery1());
		m_queries.add(new TPCHQuery3());
		m_rand = new Random();
		m_rand.setSeed(Config.RAND_SEED);
	}

	public void run() {
		while (true) {
			int queryNum = randomQuery();
			//System.out.print("Running Query "+queryNum + "...");
			DistributedQuery query = m_queries.get(queryNum);
			query.updatePartitionTable(m_coord.getCatalog());
			query.run();
			m_coord.incrementCount();
			//System.out.println("finished!");
		}
	}


	private int randomQuery(){
	    return m_rand.nextInt(m_queries.size());
	}
	
	private Random m_rand;
	private Vector<DistributedQuery> m_queries;
	private QueryCoordinator m_coord;
}
