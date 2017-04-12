package app.singleton;

import krug.daan.easynosql.mongodb.config.MongoConfig;
import krug.daan.easynosql.mongodb.dao.BaseDAO;

public class MongoDAOFactory {
	
	private static final String host = "localhost";
	private static final Integer port = 27017;
	private static final String user = null;
	private static final String password = null;
	private static final String databaseName = "test1";
	
	private static MongoConfig mgc;
	
	static{
		mgc = new MongoConfig(host, port, databaseName, user, password);
	}
	
	public static BaseDAO getBaseDAO(){
		return BaseDAO.getInstance(mgc);
	}
}
