package krug.daan.easynosql.couchdb.exception;


/**
 * @author Daniel Augusto Krug
 * 
 * Class to encapsulate the data access errors and exceptions
 * when using a CouchDB mechanism.
 */
public class CouchDataException extends Exception{
	
	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -1L; 
	
	/**
	 * Overrides a super() Constructor
	 */
	public CouchDataException(Exception e){
		super(e);
	}
	
	/**
	 * Overrides a super(String msg) Constructor
	 */
	public CouchDataException(String msg){
		super(msg);
	}
}
