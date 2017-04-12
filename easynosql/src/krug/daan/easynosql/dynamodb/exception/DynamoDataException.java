package krug.daan.easynosql.dynamodb.exception;


/**
 * @author Daniel Augusto Krug
 * 
 * Class to encapsulate the data access errors and exceptions
 * when using a DynamoDB mechanism.
 */
public class DynamoDataException extends Exception{
	
	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -1L; 
	
	/**
	 * Overrides a super() Constructor
	 */
	public DynamoDataException(Exception e){
		super(e);
	}
	
	/**
	 * Overrides a super(String msg) Constructor
	 */
	public DynamoDataException(String msg){
		super(msg);
	}
}
