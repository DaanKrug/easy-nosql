package krug.daan.easynosql.dynamodb.dao;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import krug.daan.easynosql.dynamodb.config.DynamoConfig;
import krug.daan.easynosql.dynamodb.dto.BaseDTO;
import krug.daan.easynosql.dynamodb.exception.DynamoDataException;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

/**
 * @author Daniel Augusto Krug
 *
 * Class to handle specific DynamoDB Table operations
 */
public class AwsTableDAO {
	
	/**
	 * DynamoConfig
	 */
	private DynamoConfig dnc;
	
	/**
	 * Cache of existing table names
	 */
	private List<String> existingTables;
	
	/**
	 * Singleton instance
	 */
	private static AwsTableDAO instance;
	
	/**
	 * Return the singleton instance
	 */
	public static AwsTableDAO getInstance(DynamoConfig dnc){
		if(null==instance){
			instance = new AwsTableDAO(dnc);
		}
		return instance;
	}
	
	/**
	 * Constructor
	 * @param dnc
	 */
	private AwsTableDAO(DynamoConfig dnc){
		this.dnc = dnc;
		existingTables = new ArrayList<String>();
	}
	
	/**
	 * Creates a table for a BaseDTO
	 */
	public Table createTable(BaseDTO dto) throws InterruptedException, DynamoDataException{
		 ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
	     attributeDefinitions.add(new AttributeDefinition()
	                     				.withAttributeName(BaseDTO.ID_ATTR_DESCRIPTOR)
	                     				.withAttributeType("S"));
		 ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
	     keySchema.add(new KeySchemaElement()
	     						.withAttributeName(BaseDTO.ID_ATTR_DESCRIPTOR)
	     								.withKeyType(KeyType.HASH)); 
	     CreateTableRequest request = new CreateTableRequest()
       									.withTableName(dto.getTableName())
       										.withKeySchema(keySchema)
       											.withAttributeDefinitions(attributeDefinitions)
       												.withProvisionedThroughput(new ProvisionedThroughput()
       														.withReadCapacityUnits(dnc.getReadUnits())
       															.withWriteCapacityUnits(dnc.getWriteUnits()));
	     System.out.println("Issuing CreateTable request for " + dto.getTableName());
	     Table table = dnc.getDynamoClient().createTable(request);
	     System.out.println("Waiting for " + dto.getTableName() + " to be created...this may take a while...");
	     table.waitForActive();
	     printTableInformation(dto.getTableName());
	     return table;
	}
	
	/**
	 * Describes a existing table
	 */
	public void printTableInformation(String tableName) throws DynamoDataException{
		try {
			 if(!tableExist(tableName)){
				throw new DynamoDataException("Table " + tableName + " dont exists!");
			 }
			 System.out.println("Describing " + tableName);
		     TableDescription tableDescription = dnc.getDynamoClient().getTable(tableName).describe();
		     String label = "Name: %s:\n" + "Status: %s \nProvisioned Throughput (read capacity units/sec): %d \n";
		     label += "Provisioned Throughput (write capacity units/sec): %d \n";
		     System.out.format(label, tableDescription.getTableName(),tableDescription.getTableStatus(),
		               			tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
		               				tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
		     for(KeySchemaElement kse : tableDescription.getKeySchema()){
		    	 System.out.println("kse: " + kse.getAttributeName() + " type: " + kse.getKeyType());
		     }
		} 
		catch (DynamoDataException de) {
	    	throw de;
	    }
		catch (Exception e) {
			throw new DynamoDataException(e);
		}
	}
	
	/**
	 * List all existing table names
	 */
	public void listTables() throws DynamoDataException{
		try {
			TableCollection<ListTablesResult> tables = dnc.getDynamoClient().listTables();
		    Iterator<Table> iterator = tables.iterator();
		    System.out.println("Listing table names");
		    while (iterator.hasNext()) {
		        Table table = iterator.next();
		        printTableInformation(table.getTableName());
		    }
		} catch (Exception e) {
			throw new DynamoDataException(e);
		}
	}
	
	/**
	 * Verifiy if a table exists or not
	 */
	public boolean tableExist(String tableName) throws DynamoDataException{
		try {
			if(existingTables.contains(tableName)){
				return true;
			}
			boolean exist = false;
			TableCollection<ListTablesResult> tables = dnc.getDynamoClient().listTables();
		    Iterator<Table> iterator = tables.iterator();
		    while (iterator.hasNext()) {
		        Table table = iterator.next();
		        if(table.getTableName().equals(tableName)){
		        	exist = true;
		        	existingTables.add(tableName);
		        	break;
		        }
		    }
		    return exist;
		} catch (Exception e) {
			throw new DynamoDataException(e);
		}
	}
	
	/**
	 * Update throughtput configuration values of a specific table that stores objects of class
	 * of a BaseDTO 
	 */
	public TableDescription updateThroughput(BaseDTO dto, long readUnits, long writeUnits) throws DynamoDataException{
	    try {
	    	if(!tableExist(dto.getTableName())){
				throw new DynamoDataException("Table " + dto.getTableName() + " dont exists!");
			}
	    	Table table = dnc.getDynamoClient().getTable(dto.getTableName());
		    System.out.println("Modifying provisioned throughput for " + dto.getTableName());
		    TableDescription td = table.updateTable(new ProvisionedThroughput().withReadCapacityUnits(readUnits).withWriteCapacityUnits(writeUnits));
	        table.waitForActive();
	        return td;
	    } 
	    catch (DynamoDataException de) {
	    	throw de;
	    }
	    catch (Exception e) {
	        System.err.println("updateThroughput request failed for " + dto.getTableName());
	        throw new DynamoDataException(e);
	    }
	}
	
	/**
	 * Delete a table if exists
	 */
	public DeleteTableResult deleteTable(BaseDTO dto) throws DynamoDataException{
	    try {
	    	if(!tableExist(dto.getTableName())){
				throw new DynamoDataException("Table " + dto.getTableName() + " dont exists!");
			}
	    	Table table = dnc.getDynamoClient().getTable(dto.getTableName());
	    	System.out.println("Issuing DeleteTable request for " + dto.getTableName());
    		DeleteTableResult dtr = table.delete();

	        System.out.println("Waiting for " + dto.getTableName()
	                   + " to be deleted...this may take a while...");

	        table.waitForDelete();
	        if(existingTables.contains(dto.getTableName())){
				existingTables.remove(dto.getTableName());
			}
	        return dtr;
	    } 
	    catch (DynamoDataException de) {
	    	throw de;
	    }
	    catch (Exception e) {
	        System.err.println("DeleteTable request failed for " + dto.getTableName());
	        throw new DynamoDataException(e);
	    }
	}

}
