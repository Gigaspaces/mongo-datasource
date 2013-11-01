package com.gigaspaces.persistency;

import com.gigaspaces.logger.GSLogConfigLoader;
import com.gigaspaces.persistency.helper.EmbeddedMongoController;

public class MongoTestServer {

	private String dbName = "space";
	private int port;
	private final EmbeddedMongoController mongoController = new EmbeddedMongoController();

	/**
	 * @param isEmbedded
	 *            - run Mongo in this process. Use for debugging only since
	 *            causes leaks.
	 */
	public void initialize(boolean isEmbedded) {
		if (MongoTestSuite.isSuiteMode()) {
			dbName = MongoTestSuite.createDatabaseAndReturnItsName();
			port = MongoTestSuite.getPort();
		} else {
			GSLogConfigLoader.getLoader();
			mongoController.initMongo(isEmbedded);

			mongoController.createDb(dbName);

			port = mongoController.getPort();
		}
	}

	public void destroy() {
		if (!MongoTestSuite.isSuiteMode()) {
			mongoController.stopMongo();
		}
	}

	public int getPort() {
		return port;
	}

	public String getHost() {

		return "localhost";
	}

	public String getDBName() {
		return dbName;
	}
}
