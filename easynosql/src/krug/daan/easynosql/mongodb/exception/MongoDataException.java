package krug.daan.easynosql.mongodb.exception;


/**
 * @author Daniel Augusto Krug
 * 
 * Class to encapsulate the data access errors and exceptions
 * when using a MongoDB mechanism.
 */
public class MongoDataException extends Exception{
	
	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -1L; 
	
	/**
	 * Overrides a super() Constructor
	 */
	public MongoDataException(Exception e){
		super(e);
	}
	
	/**
	 * Overrides a super(String msg) Constructor
	 */
	public MongoDataException(String msg){
		super(msg);
	}
}
