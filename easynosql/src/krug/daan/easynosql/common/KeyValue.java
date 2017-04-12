package krug.daan.easynosql.common;

/**
 * @author Daniel Augusto Krug
 * 
 * Class to simplify the mapping values of a key-value pairs, on handling
 * of diverse database operations.
 */
public class KeyValue {
	
	/**
	 * The key
	 */
	protected String key;
	
	/**
	 * The value
	 */
	protected Object value;
	
	/**
	 * Constructor
	 * 
	 */
	public KeyValue(String key, Object value) {
		super();
		this.key = key;
		this.value = value;
	}
	
	/**
	 * @return the key
	 * 
	 */
	public String getKey() {
		return key;
	}

	/**
	 * 
	 * @return the value
	 */
	public Object getValue() {
		return value;
	}
}
