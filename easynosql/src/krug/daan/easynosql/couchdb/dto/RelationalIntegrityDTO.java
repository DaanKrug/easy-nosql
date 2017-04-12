package krug.daan.easynosql.couchdb.dto;

/**
 * @author Daniel Augusto Krug
 * 
 * Class to handling of relations between stored objects.
 */
public class RelationalIntegrityDTO extends BaseDTO {
	
	/**
	 * OWNER_TABLE_NAME
	 */
	public static final String OWNER_TABLE_NAME = "ownerTableName";
	
	/**
	 * OWNER_ID
	 */
	public static final String OWNER_ID = "ownerId";
	
	/**
	 * RELATED_TABLE_NAME
	 */
	public static final String RELATED_TABLE_NAME = "relatedTableName";
	
	/**
	 * RELATED_ID
	 */
	public static final String RELATED_ID = "relatedId";
	
	/**
	 * Table name of object owner. 
	 * Owner contains the relational reference to related object
	 * in the [relatedObjectsIds] attribute.
	 */
	private String ownerTableName;
	
	/**
	 * "_id" value of object owner. 
	 * Owner contains the relational reference to related object
	 * in the [relatedObjectsIds] attribute.
	 */
	private String ownerId;
	
	/**
	 * Table name of related object on owner object
	 */
	private String relatedTableName;
	
	/**
	 * "_id" value of related object on owner object
	 */
	private String relatedId;
	
	/**
	 * Constructor
	 */
	public RelationalIntegrityDTO(){
		super(RelationalIntegrityDTO.class);
	}
	
	/**
	 * @return ownerTableName
	 */
	public String getOwnerTableName() {
		return ownerTableName;
	}
	
	/**
	 * Set the ownerTableName
	 * @param ownerTableName
	 */
	public void setOwnerTableName(String ownerTableName) {
		this.ownerTableName = ownerTableName;
	}
	
	/**
	 * @return ownerId
	 */
	public String getOwnerId() {
		return ownerId;
	}
	
	/**
	 * Set the ownerId
	 * @param ownerId
	 */
	public void setOwnerId(String ownerId) {
		this.ownerId = ownerId;
	}
	
	/**
	 * @return relatedTableName
	 */
	public String getRelatedTableName() {
		return relatedTableName;
	}
	
	/**
	 * Set the relatedTableName
	 * @param relatedTableName
	 */
	public void setRelatedTableName(String relatedTableName) {
		this.relatedTableName = relatedTableName;
	}
	
	/**
	 * @return relatedId
	 */
	public String getRelatedId() {
		return relatedId;
	}
	
	/**
	 * Set the relatedId
	 * @param relatedId
	 */
	public void setRelatedId(String relatedId) {
		this.relatedId = relatedId;
	}
}
