package krug.daan.easynosql.cassandradb.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import krug.daan.easynosql.cassandradb.config.CassandraConfig;
import krug.daan.easynosql.cassandradb.dto.BaseDTO;
import krug.daan.easynosql.cassandradb.exception.CassandraDataException;
import krug.daan.easynosql.cassandradb.util.DtoUtil;
import krug.daan.easynosql.common.KeyValue;

import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.InvalidQueryException;

/**
 * @author Daniel Augusto Krug
 *
 * Class to handle specific CassandraDB functions.
 */
public class CassandraDAO {
	
	/**
	 * The CassandraConfig
	 */
	private CassandraConfig csc;
	
	/**
	 * Cache of table names
	 */
	private List<String> existingTables;
	
	/**
	 * Actual database name
	 */
	private String databaseInUse;
	
	/**
	 * UNKNOW_KEYSPACE_REGEX
	 */
	private static final String UNKNOW_KEYSPACE_REGEX = "Unknown keyspace .*?";
	
	/**
	 * KEYSPACE_DONT_EXISTS_REGEX
	 */
	private static final String KEYSPACE_DONT_EXISTS_REGEX = "Keyspace .*? does not exist";
	
	/**
	 * CANNOT_DROP_NON_EXISTING_KEYSPACE_REGEX
	 */
	private static final String CANNOT_DROP_NON_EXISTING_KEYSPACE_REGEX = "Cannot drop non existing keyspace .*?";
	
	/**
	 * INVALID_COLUMN_NAME_TO_ADD_REGEX
	 */
	private static final String INVALID_COLUMN_NAME_TO_ADD_REGEX = "Invalid column name .*? because it conflicts with an existing column";
	
	/**
	 * INVALID_COLUMN_NAME_TO_DROP_REGEX
	 */
	private static final String INVALID_COLUMN_NAME_TO_DROP_REGEX = "Column .*? was not found in table .*?";
	
	/**
	 * UNCONFIGURED_TABLE_REGEX
	 */
	private static final String UNCONFIGURED_TABLE_REGEX = "Unconfigured table .*?";
	
	/**
	 * UNDEFINED_COLUMN_NAME_REGEX
	 */
	private static final String UNDEFINED_COLUMN_NAME_REGEX = "Undefined column name .*?";
	
	/**
	 * INDEX_NOT_FOUND_REGEX
	 */
	private static final String INDEX_NOT_FOUND_REGEX = "Index .*? could not be found in any of the tables of keyspace .*?";
	
	/**
	 * Singleton instance
	 */
	private static CassandraDAO instance;
	
	/**
	 * Return the singleton instance
	 */
	public static CassandraDAO getInstance(CassandraConfig csc){
		if(null==instance){
			instance = new CassandraDAO(csc);
		}
		return instance;
	}
	
	/**
	 * Constructor
	 */
	private CassandraDAO(CassandraConfig csc){
		this.csc = csc;
		this.existingTables = new ArrayList<String>();
		this.databaseInUse = null;
	}
	
	/**
	 * Verify if the exception should be re-throw or not.
	 */
	private boolean donothingOnError(InvalidQueryException iqe, String regex){
		String errorMessage1 = "";
		String errorMessage2 = "";
		if(null!=iqe.getMessage()){
			errorMessage1 = iqe.getMessage();
		}
		if(null!=iqe.getCause() && null!=iqe.getCause().getMessage()){
			errorMessage2 = iqe.getCause().getMessage();
		}
		Pattern p = Pattern.compile(regex);
		Matcher m1 = p.matcher(errorMessage1);
		Matcher m2 = p.matcher(errorMessage2);
		return ((m1.matches() || m2.matches()));
	}
	
	/**
	 * Creates a new database or update if already exists: networkStrategy, replicationFactor
	 * and durableWrites values.
	 */
	public boolean createOrUpdateDatabase(String dbName, 
			boolean networkStrategy, int replicationFactor,
			boolean durableWrites,boolean createNew,boolean ignoreIfAlreadyExists) throws CassandraDataException{
		try {
			String query = "CREATE";
			if(!createNew){
				query = "ALTER";
			}
			query += " KEYSPACE ";
		    query += dbName;
		    query += " WITH replication = {'class':";
		    if(networkStrategy){
		    	query += "'NetworkTopologyStrategy'";
		    }else{
		    	query += "'SimpleStrategy', 'replication_factor':";
				query += replicationFactor;
		    }
		    query += "} AND DURABLE_WRITES = ";
		    if(durableWrites){
		    	query += "true";
		    }else{
		    	query += "false";
		    }
		    query += ";";
		    try {
		    	csc.getSession().execute(query);
			} catch (AlreadyExistsException aee){
				String msg = "Database(keyspace) [" + dbName + "] could not be created, because already exists.";
				if(ignoreIfAlreadyExists){
					csc.logWarn(msg);
					return true;
				}else{
					throw new CassandraDataException(msg);
				}
			} catch(InvalidQueryException iqe){
				if(donothingOnError(iqe,UNKNOW_KEYSPACE_REGEX)){
					//do nothing, the keyspace dont exists to be updated. No need re-throw a exception
					String msg = "Database(keyspace) [" + dbName + "] could not be updated, because dont exists.";
					csc.logError(msg);
					return false;
				}else {
					throw new CassandraDataException(iqe);
				}
			}
		    String msg = "Database(keyspace) [" + dbName + "] sucessfully "+ (createNew ? "created." : "updated.");
		    csc.logInfo(msg);
		    return true;
		} 
		catch (CassandraDataException cde){
			throw cde;
		}
		catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Drop a existing database
	 */
	public boolean dropDatabase(String dbName) throws CassandraDataException{
		try {
			String query = "DROP KEYSPACE " + dbName + ";";
			csc.getSession().execute(query);
			csc.logInfo("Database(keyspace) [" + dbName + "] sucessfully deleted.");
			return true;
		} 
		catch(InvalidQueryException iqe){
			if(donothingOnError(iqe,CANNOT_DROP_NON_EXISTING_KEYSPACE_REGEX)){
				//do nothing, the keyspace dont exists to be deleted. No need re-throw a exception
				String msg = "Database(keyspace) [" + dbName + "] could not be deleted, because dont exists.";
				csc.logError(msg);
				return false;
			}else{
				throw new CassandraDataException(iqe);
			}
		}
		catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Define the database to be used to execute the CQL (Cassandra Query Language) commands
	 */
	public boolean useDatabase(String dbName) throws CassandraDataException{
		String lastDataBaseInUse = databaseInUse;
		try{
			if(null==databaseInUse || !(databaseInUse.equals(dbName))){
				csc.getSession().execute("USE " + dbName );
			    csc.logInfo("Using database(keyspace) [" + dbName + "].");
			    databaseInUse = dbName;
			    existingTables = new ArrayList<String>();
			}else{
				csc.logInfo("Database(keyspace) [" + dbName + "] already is actually in use, no changes required.");
			}
		    return true;
		} 
		catch(InvalidQueryException iqe){
			databaseInUse = lastDataBaseInUse;
			if(donothingOnError(iqe,KEYSPACE_DONT_EXISTS_REGEX)){
				//do nothing, the keyspace dont exists to be used. No need re-throw a exception
				String msg = "Database(keyspace) [" + dbName + "] could not be used, because dont exists.";
				csc.logError(msg);
				return false;
			}else{
				throw new CassandraDataException(iqe);
			}
		}
		catch (Exception e) {
			databaseInUse = lastDataBaseInUse;
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Creates a new table according the received BaseDTO object
	 */
	public boolean createTable(BaseDTO dto, boolean ignoreIfAlreadyExists) throws CassandraDataException{
		try {
			if(existingTables.contains(dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1))){
				String msg = "Table [" +  dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + "] could not be created, because already exists.";
				if(ignoreIfAlreadyExists){
					csc.logWarn(msg);
					return true;
				}else{
					throw new CassandraDataException(msg);
				}
			}
			if(!createOrUpdateDatabase(csc.getDatabaseName(), csc.isNetworkStrategy(), csc.getReplicationFactor(), csc.isDurableWrites(), true, true)){
				return false;
			}
			if(!useDatabase(csc.getDatabaseName())){
				return false;
			}
			try {
				List<String[]> tableMetadata = DtoUtil.generateTableMetadata(dto);
				String virgula = "";
				StringBuffer query = new StringBuffer();
				query.append("CREATE TABLE " + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + "(");
				for(String[] mData : tableMetadata){
					query.append(virgula);
					query.append(mData[0].trim());
					query.append(" ");
					query.append(mData[1].trim());
					query.append(" ");
					query.append(mData[2].trim());
					virgula = ",";
				}
				query.append(");");
				csc.getSession().execute(query.toString());
			    csc.logInfo(query.toString());
			    //
			    for(String[] mData : tableMetadata){
			    	String secondaryIndexesQuery = "";
			    	if(mData[3].equals("true")){
			    		secondaryIndexesQuery = " CREATE INDEX ON " + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + "(" + mData[0].trim() + ")";
			    	}
			    	if(secondaryIndexesQuery.trim().length() > 0){
				    	csc.getSession().execute(secondaryIndexesQuery);
					    csc.logInfo(secondaryIndexesQuery);
				    }
			    }
			}catch (AlreadyExistsException aee){
				String msg = "Table [" +  dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + "] could not be created, because already exists.";
				if(ignoreIfAlreadyExists){
					csc.logError(msg);
					return true;
				}else{
					throw new CassandraDataException(msg);
				}
			} 
			existingTables.add(dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1));
			return true;
		} 
		catch (CassandraDataException cde) {
			throw cde;
		}
		catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Drop a existing table, respective a received BaseDTO object
	 */
	public boolean dropTable(BaseDTO dto) throws CassandraDataException{
		try {
			if(!useDatabase(csc.getDatabaseName())){
				return false;
			}
			try {
				String query = "DROP TABLE " + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + ";";
				csc.getSession().execute(query);
			    csc.logInfo("TABLE [" + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + "] sucessfully dropped.");
			    if(existingTables.contains(dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1))){
			    	existingTables.remove(dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1));
			    }
			    return true;
			}catch(InvalidQueryException iqe){
				if(donothingOnError(iqe,UNCONFIGURED_TABLE_REGEX)){
					//do nothing, the table dont exists. No need re-throw a exception
					String msg = "TABLE [" + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + "] could not be dropped, because dont exists.";
					csc.logError(msg);
					return false;
				}else{
					throw new CassandraDataException(iqe);
				}
			}
		} 
		catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Drop or add columns in a existing table, respective a received BaseDTO object.
	 */
	@SuppressWarnings(value="all")
	public boolean alterTable(BaseDTO dto,List<KeyValue> columns, boolean toDrop) throws CassandraDataException{
		try {
			if(!useDatabase(csc.getDatabaseName())){
				return false;
			}
			for(KeyValue keyValue: columns){
				String query = "ALTER TABLE " + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + "";
				if(toDrop){
					query += " DROP ";
					query += keyValue.getKey();
				}else{
					query += " ADD ";
					query += keyValue.getKey() + " " + DtoUtil.getDataTypeForAttributeClass((Class)keyValue.getValue());
				}
				csc.logInfo(query);
				try {
					csc.getSession().execute(query);
					String msg = "TABLE [" + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + "] column: [";
					msg += keyValue.getKey() + "] sucessfully " + (toDrop ? "dropped." : "added.");
					csc.logInfo(msg);
				} catch(InvalidQueryException iqe){
					if(donothingOnError(iqe,UNCONFIGURED_TABLE_REGEX)){
						//do nothing, the table dont exists. No need re-throw a exception
						String msg = "Column name [" + keyValue.getKey() + "] could not be ";
						msg += (toDrop ? "dropped" : "added") + " because TABLE [" + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + "] dont exists.";
						csc.logError(msg);
						return false;
					}else if(donothingOnError(iqe,INVALID_COLUMN_NAME_TO_ADD_REGEX)){
						//do nothing, the column already exists. No need re-throw a exception
						String msg = "Column name [" + keyValue.getKey() + "] on TABLE [" + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + "] could not be created, because already exists.";
						csc.logError(msg);
					}else if(donothingOnError(iqe,INVALID_COLUMN_NAME_TO_DROP_REGEX)){
						//do nothing, the column dont exists to be deleted. No need re-throw a exception
						String msg = "Column name [" + keyValue.getKey() + "] on TABLE [" + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + "] could not be dropped, because dont exists.";
						csc.logError(msg);
					}else{
						throw new CassandraDataException(iqe);
					}
				}
			}
			return true;
		} 
		catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Create or remove a column index on a table, respective to a received BaseDTO object
	 */
	public boolean alterIndex(BaseDTO dto, String attrName,String indexName, boolean toDrop) throws CassandraDataException{
		try {
			if(!useDatabase(csc.getDatabaseName())){
				return false;
			}
			String query = "";
			if(!toDrop){
				query = "CREATE INDEX " + indexName + " ON " + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + " (" + attrName + ");";
			}else{
				query = "DROP INDEX " + indexName + ";";
			}
			try{
				csc.getSession().execute(query);
			} catch(InvalidQueryException iqe){
				if(donothingOnError(iqe,UNDEFINED_COLUMN_NAME_REGEX)){
					//do nothing, the column name dont exists. No need re-throw a exception
					String msg = "Index [" + indexName + "] could not be created, because column [";
					msg += attrName + "] on TABLE ["+ dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) +"] dont exists.";
					csc.logError(msg);
					return false;
				}else if(donothingOnError(iqe,INDEX_NOT_FOUND_REGEX)){
					//do nothing, the column already exists. No need re-throw a exception
					String msg = "Index [" + indexName + "] could not be dropped, because dont";
					msg += " exist on any TABLE of database [" + databaseInUse +"].";
					csc.logError(msg);
					return false;
				}else{
					throw new CassandraDataException(iqe);
				}
			}
			String msg = "Index [" + attrName + "] on TABLE [" +  dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + "] sucessfully " + (!toDrop ? "created." : "dropped.");
			csc.logInfo(msg);
			return true;
		} catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Disconnect from the CassandraDB mechanism
	 */
	public void shutdown(){
		csc.shutdown();
		csc.logInfo("CassandraDB was shutdown."); 
	}

}
