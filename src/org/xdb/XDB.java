package org.xdb;

import org.xdb.elasticity.QueryCoordinator;

public class XDB {

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Usage: XDB <dbname>");
			return;
		}
		
		//change config
		Config.DB_NAME = args[0];
		
		//run
		QueryCoordinator coord = new QueryCoordinator(1);
		coord.start();
	}
}
