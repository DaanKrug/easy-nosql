package app.singleton;

import java.util.logging.Level;

import krug.daan.easynosql.cassandradb.config.CassandraConfig;
import krug.daan.easynosql.cassandradb.dao.BaseDAO;
import krug.daan.easynosql.cassandradb.dao.CassandraDAO;


public class CassandraDAOFactory {
	
	private static final String host = "localhost";
	private static final Integer port = 9042;//9160;
	private static final String user = null;
	private static final String password = null;
	private static final String databaseName = "TestCassandra23";
	private static final Level logLevel = Level.INFO;
	
	private static final boolean networkStrategy = false;
	private static final int replicationFactor = 1;
	private static final boolean durableWrites = true;
	
	private static CassandraConfig csc;
	
	static{
		csc = new CassandraConfig(host, port,databaseName,
				 networkStrategy, replicationFactor,durableWrites, user, password,logLevel);
	}
	
	public static CassandraDAO getCassandraDAO(){
		return CassandraDAO.getInstance(csc);
	}
	
	public static BaseDAO getBaseDAO(){
		return BaseDAO.getInstance(csc);
	}
}
