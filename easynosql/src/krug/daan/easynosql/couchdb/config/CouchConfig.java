package krug.daan.easynosql.couchdb.config;

import java.util.logging.Level;

import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;

/**
 * @author Daniel Augusto Krug
 * 
 * Class to encapsulate the basic configuration parameters for a CouchDB mechanism.
 */
public class CouchConfig {

	/**
	 * CouchDB Host
	 */
	private String host;
	
	/**
	 * CouchDB port
	 */
	private Integer port;
	
	/**
	 * CouchDB user
	 */
	private String user;
	
	/**
	 * CouchDB password
	 */
	private String password;
	
	/**
	 * CouchDB database name
	 */
	private String databaseName;
	
	/**
	 * CouchDB protocol default = [HTTP]
	 */
	private String protocol;
	
	/**
	 * CouchDB createdb if-not-exist
	 */
	private boolean createDatabaseIfInexist;
	
	/**
	 * CouchDB connection timeout
	 */
	private int connectionTimeOut;
	
	/**
	 * CouchDB maximum number of opened connections
	 */
	private int maxConnections;
	
	/**
	 * CouchDB properties object
	 */
	private CouchDbProperties properties;
	
	/**
	 * CouchDb client
	 */
	private CouchDbClient dbClient;
	
	/**
	 * Log level
	 */
	private Level logLevel;
	
	/**
	 * Constructor
	 * 
	 * @param host
	 * @param port
	 * @param databaseName
	 * @param user
	 * @param password
	 * @param logLevel
	 */
	public CouchConfig(String host,Integer port,String databaseName, String user, String password, Level logLevel){
		this.host = host;
		this.port = port;
		this.databaseName = databaseName;
		this.user = user;
		this.password = password;
		this.protocol = "http";
		this.createDatabaseIfInexist = true;
		this.connectionTimeOut = 0;
		this.maxConnections = 100;
		this.logLevel = (null!=logLevel ? logLevel : Level.SEVERE);
		initialize();
	}
	
	/**
	 * Constructor
	 * 
	 * @param host
	 * @param port
	 * @param databaseName
	 * @param user
	 * @param password
	 * @param protocol
	 * @param createDatabaseIfInexist
	 * @param logLevel
	 */
	public CouchConfig(String host,Integer port,String databaseName, 
			String user, String password, String protocol, boolean createDatabaseIfInexist, Level logLevel){
		this.host = host;
		this.port = port;
		this.databaseName = databaseName;
		this.user = user;
		this.password = password;
		this.protocol = protocol;
		this.createDatabaseIfInexist = createDatabaseIfInexist;
		this.connectionTimeOut = 0;
		this.maxConnections = 100;
		this.logLevel = (null!=logLevel ? logLevel : Level.SEVERE);
		initialize();
	}
	
	/**
	 * Constructor
	 * 
	 * @param host
	 * @param port
	 * @param databaseName
	 * @param user
	 * @param password
	 * @param protocol
	 * @param createDatabaseIfInexist
	 * @param connectionTimeOut
	 * @param maxConnections
	 * @param logLevel
	 */
	public CouchConfig(String host,Integer port,String databaseName, 
			String user, String password, String protocol, boolean createDatabaseIfInexist, 
			int connectionTimeOut, int maxConnections, Level logLevel){
		this.host = host;
		this.port = port;
		this.databaseName = databaseName;
		this.user = user;
		this.password = password;
		this.protocol = protocol;
		this.createDatabaseIfInexist = createDatabaseIfInexist;
		this.connectionTimeOut = connectionTimeOut;
		this.maxConnections = maxConnections;
		this.logLevel = (null!=logLevel ? logLevel : Level.SEVERE);
		initialize();
	}
	
	/**
	 * Initialization of parameters on moment of construction
	 * 
	 */
	private void initialize() {
		java.util.logging.Logger.getLogger("org.lightcouch").setLevel(logLevel);
		java.util.logging.Logger.getLogger("org.apache.http").setLevel(logLevel);
		
		properties = new CouchDbProperties();
		properties.setHost(host);
		properties.setPort(port);
		properties.setDbName(databaseName);
		if(null!=user && user.trim().length() > 0 && null!=password && password.trim().length() > 0){
			properties.setUsername(user);
			properties.setPassword(password);
		}
		properties.setProtocol(protocol);
		properties.setCreateDbIfNotExist(createDatabaseIfInexist);
		if(connectionTimeOut > 0){
			properties.setConnectionTimeout(connectionTimeOut);
		}
		if(maxConnections > 0){
			properties.setMaxConnections(maxConnections);
		}
		dbClient = new CouchDbClient(properties);
	}
	
	/**
	 * Desalocate the resources
	 */
	public void shutdown(){
		dbClient.shutdown();
		properties = null;
	}

	/**
	 * Returns the dbClient object that allows CouchDb access
	 * @return dbClient
	 */
	public CouchDbClient getDbClient() {
		return dbClient;
	}
}
