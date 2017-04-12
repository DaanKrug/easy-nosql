package krug.daan.easynosql.dynamodb.config;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

/**
 * @author Daniel Augusto Krug
 *
 * Encapsulates the DynamoDB data access mechanism
 */
public class DynamoConfig {
	
	/**
	 * DynamoDB 
	 */
	private DynamoDB dynamoDB;
	
	/**
	 * Mapper
	 */
	private DynamoDBMapper mapper;
	
	/**
	 * DynamoDB Read units capacity configuration
	 */
	private long readUnits;
	
	/**
	 * DynamoDB Write units capacity configuration
	 */
	private long writeUnits;
	
	/**
	 * Constructor 
	 * 
	 * @param region
	 * @param profileName
	 * @param readUnits
	 * @param writeUnits
	 */
	public DynamoConfig(Regions region,String profileName,long readUnits,long writeUnits){
		AmazonDynamoDB dynamoClient = null;
		if(null!=region){
			AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard().withRegion(region);
			if(null!=profileName && profileName.trim().length() > 0){
				builder.setCredentials(new ProfileCredentialsProvider(profileName));
			}
			dynamoClient  = builder.build();
		}else{
			EndpointConfiguration epc = new EndpointConfiguration("http://localhost:8000", "");
			AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
			builder.setEndpointConfiguration(epc);
			BasicAWSCredentials awsCreds = new BasicAWSCredentials("", "");
			builder.setCredentials(new AWSStaticCredentialsProvider(awsCreds));
			dynamoClient = builder.build();
		}
		dynamoDB = new DynamoDB(dynamoClient);
		mapper = new DynamoDBMapper(dynamoClient);
		this.readUnits = readUnits;
		this.writeUnits = writeUnits;
	}
	
	/**
	 * Shutdown method
	 */
	public void shutdown(){
		dynamoDB.shutdown();
	}

	/**
	 * @return dynamoDB
	 */
	public DynamoDB getDynamoClient() {
		return dynamoDB;
	}

	/**
	 * @return readUnits
	 */
	public long getReadUnits() {
		return readUnits;
	}

	/**
	 * @return writeUnits
	 */
	public long getWriteUnits() {
		return writeUnits;
	}

	/**
	 * @return mapper
	 */
	public DynamoDBMapper getMapper() {
		return mapper;
	}
	
}
