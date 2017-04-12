package krug.daan.easynosql.cassandradb.config;

import java.util.logging.Level;

import java.util.logging.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * @author Daniel Augusto Krug
 * 
 * Class to encapsulate the basic configuration parameters for a CassandraDB mechanism.
 */
public class CassandraConfig {

	/**
	 * CassandraDB Host
	 */
	private String host;
	
	/**
	 * CassandraDB port
	 */
	private Integer port;
	
	/**
	 * CassandraDB database to connect
	 */
	private String databaseName;
	
	/**
	 * CassandraDB user
	 */
	private String user;
	
	/**
	 * CassandraDB password
	 */
	private String password;
	
	/**
	 * CassandraDB cluster
	 */
	private Cluster cluster;
	
	/**
	 * The log level
	 */
	private Level logLevel;
	
	/**
	 * CassandraDB session
	 */
	private Session session;
	
	/**
	 * CassandraDB networkStrategy
	 */
	private boolean networkStrategy;
	
	/**
	 * CassandraDB replicationFactor
	 * It is the number of machines in the cluster that will receive copies of the same data.
	 */
	private int replicationFactor;
	
	/**
	 * CassandraDB durableWrites
	 */
	private boolean durableWrites;
	
	/**
	 * The package name for logger
	 */
	private static final String packageLogName = "krug.daan.easynosql.cassandradb";
	
	/**
	 * Log messages
	 */
	private static Logger LOG = Logger.getLogger(packageLogName);
	
	/**
	 * Constructor 
	 * 
	 * @param host
	 * @param port
	 * @param user
	 * @param password
	 * @param logLevel
	 */
	public CassandraConfig(String host,Integer port, String databaseName, 
							boolean networkStrategy, Integer replicationFactor, boolean durableWrites,
							String user, String password, Level logLevel){
		this.host = host;
		this.port = port;
		this.databaseName = databaseName;
		this.user = user;
		this.password = password;
		this.networkStrategy = networkStrategy;
		this.replicationFactor = replicationFactor;
		this.durableWrites = durableWrites;
		this.logLevel = (null!=logLevel ? logLevel : Level.INFO);
	}
	
	/**
	 * Initialization of parameters on moment of construction
	 */
	private void initialize() {
		java.util.logging.Logger.getLogger(packageLogName).setLevel(logLevel);
		if(null!=user && user.trim().length() > 0 && null!=password && password.trim().length() > 0){
			cluster = Cluster.builder().addContactPoint(host).withPort(port).withCredentials(user, password).build();
		}else{
			cluster = Cluster.builder().addContactPoint(host).withPort(port).build();
		}
	}
	
	/**
	 * @return session
	 */
	public Session getSession() {
		if(null==cluster || cluster.isClosed()){
			initialize();
		}
		if(null==session || session.isClosed()){
			session = cluster.connect();
		}
		return session;
	}
	
	/**
	 * Shutdown the CassandraDB mechanism
	 */
	public void shutdown(){
		if(null!=session && !session.isClosed()){
			session.close();
		}
		if(null!=cluster && !cluster.isClosed()){
			cluster.close();
		}
	}
	
	/**
	 * Log the info msg
	 * @param msg
	 */
	public void logInfo(String msg){
		LOG.info(msg);
	}
	
	/**
	 * Log the erro msg
	 * @param msg
	 */
	public void logError(String msg){
		LOG.severe(msg);
	}
	
	/**
	 * Log the warning msg
	 * @param msg
	 */
	public void logWarn(String msg){
		LOG.warning(msg);
	}

	/**
	 * Return the databaseName
	 */
	public String getDatabaseName() {
		return databaseName;
	}

	/**
	 * Return networkStrategy
	 */
	public boolean isNetworkStrategy() {
		return networkStrategy;
	}

	/**
	 * Set the networkStrategy
	 * @param networkStrategy
	 */
	public void setNetworkStrategy(boolean networkStrategy) {
		this.networkStrategy = networkStrategy;
	}

	/**
	 * Return the replicationFactor
	 */
	public int getReplicationFactor() {
		return replicationFactor;
	}

	/**
	 * Set the replicationFactor
	 * @param replicationFactor
	 */
	public void setReplicationFactor(int replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	/**
	 * Return durableWrites
	 */
	public boolean isDurableWrites() {
		return durableWrites;
	}

	/**
	 * Set the durableWrites
	 * @param durableWrites
	 */
	public void setDurableWrites(boolean durableWrites) {
		this.durableWrites = durableWrites;
	}

}
