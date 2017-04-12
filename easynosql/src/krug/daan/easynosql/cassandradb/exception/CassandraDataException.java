package krug.daan.easynosql.cassandradb.exception;


/**
 * @author Daniel Augusto Krug
 * 
 * Class to encapsulate the data access errors and exceptions
 * when using a CassandraDB mechanism.
 */
public class CassandraDataException extends Exception{
	
	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -1L; 
	
	/**
	 * Overrides a super() Constructor
	 */
	public CassandraDataException(Exception e){
		super(e);
	}
	
	/**
	 * Overrides a super(String msg) Constructor
	 */
	public CassandraDataException(String msg){
		super(msg);
	}
}
