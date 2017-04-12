package krug.daan.easynosql.mongodb.config;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * @author Daniel Augusto Krug
 * 
 * Class to encapsulate the basic configuration parameters for a MongoDB mechanism.
 */
public class MongoConfig {

	/**
	 * MongoDB Host
	 */
	private String host;
	
	/**
	 * MongoDB port
	 */
	private Integer port;
	
	/**
	 * MongoDB useer
	 */
	private String user;
	
	/**
	 * MongoDB password
	 */
	private String password;
	
	/**
	 * MongoDB database name
	 */
	private String databaseName;
	
	/**
	 * MongoDatabase object
	 */
	private MongoDatabase db;
	
	/**
	 * MongoClient object
	 */
	private MongoClient mongoClient;
	
	/**
	 * Constructor 
	 * 
	 * @param host
	 * @param port
	 * @param databaseName
	 * @param user
	 * @param password
	 * 
	 */
	public MongoConfig(String host,Integer port,String databaseName, String user, String password){
		this.host = host;
		this.port = port;
		this.databaseName = databaseName;
		this.user = user;
		this.password = password;
		initialize();
	}
	
	/**
	 * Initialization of parameters on moment of construction
	 * 
	 */
	private void initialize() {
		List<ServerAddress> servers = new ArrayList<ServerAddress>();
		List<MongoCredential> credentials = new ArrayList<MongoCredential>();
		if(null!=user && user.trim().length() > 0 && null!=password && password.trim().length() > 0){
			MongoCredential mc = MongoCredential.createCredential(user, databaseName, password.toCharArray());
			credentials.add(mc);
		}
		ServerAddress sa = new ServerAddress(host,port);
		servers.add(sa);
		mongoClient = new MongoClient(servers,credentials);
		db = mongoClient.getDatabase(databaseName);
	}
	
	/**
	 * Get a MongoCollection of Document by a table name
	 */
	public MongoCollection<Document> getCollection(String tableName){
		return db.getCollection(tableName);
	}
}
