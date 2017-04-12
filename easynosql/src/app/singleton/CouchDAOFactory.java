package app.singleton;

import java.util.logging.Level;

import krug.daan.easynosql.couchdb.config.CouchConfig;
import krug.daan.easynosql.couchdb.dao.BaseDAO;


public class CouchDAOFactory {
	
	private static final String host = "localhost";
	private static final Integer port = 5984;
	private static final String user = null;
	private static final String password = null;
	private static final String databaseName = "test4";
	private static final Level logLevel = Level.SEVERE;
	
	private static CouchConfig chc;
	
	static{
		chc = new CouchConfig(host, port, databaseName, user, password,logLevel);
	}
	
	public static BaseDAO getBaseDAO(){
		return BaseDAO.getInstance(chc);
	}
}
