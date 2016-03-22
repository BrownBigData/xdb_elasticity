package org.xdb;

import org.xdb.elasticity.QueryCoordinator;

public class XDB {

	public static void main(String[] args) {
		QueryCoordinator coord = new QueryCoordinator(1);
		coord.start();
	}
}
