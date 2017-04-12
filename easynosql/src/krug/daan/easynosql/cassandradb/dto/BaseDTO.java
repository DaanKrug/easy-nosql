package krug.daan.easynosql.cassandradb.dto;

import java.util.ArrayList;
import java.util.List;

import krug.daan.easynosql.cassandradb.annotation.NonPersistent;
import krug.daan.easynosql.cassandradb.exception.CassandraDataException;
import krug.daan.easynosql.common.KeyValue;



/**
 * @author Daniel Augusto Krug
 * 
 * Class to be extended by CassandraDB persistent objects.
 * Encapsulate the data for BaseDAO handle on database access.
 */
public class BaseDTO {
	
	/**
	 * ID_ATTR_DESCRIPTOR
	 */
	@NonPersistent
	public static final String ID_ATTR_DESCRIPTOR = "id";
	
	/**
	 * TABLE_NAME_ATTR_DESCRIPTOR
	 */
	@NonPersistent
	public static final String TABLE_NAME_ATTR_DESCRIPTOR = "tableName";
	
	/**
	 * KEYVALUES_ATTR_RELATED_OBJECTS_IDS
	 */
	@NonPersistent
	public static final String KEYVALUES_ATTR_RELATED_OBJECTS_IDS = "relatedObjectsIds";
	
	/**
	 * KEYVALUES_ATTR_RELATED_OBJECTS
	 */
	@NonPersistent
	public static final String KEYVALUES_ATTR_RELATED_OBJECTS = "relatedObjects";
	
	/**
	 * KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON
	 */
	@NonPersistent
	public static final String KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON = "relatedObjectsIdsJson";
	
	/**
	 * The table name mapping. 
	 */
	protected String tableName;
	
	/**
	 * The object identifier "id"
	 */
	protected String id;
	
	/**
	 * Map the related objects ids of others classes. Emulates the Object-Relational model.
	 */
	@NonPersistent
	protected List<KeyValue> relatedObjectsIds;
	
	/**
	 * Map the related objects of others classes. Populated only when object is read from database,
	 * by a search on others tables according the [relatedObjectsIds] information
	 */
	@NonPersistent
	protected List<KeyValue> relatedObjects;
	
	/**
	 * Map the List of KeyValue [relatedObjectsIds] to a unique String object, to be possible
	 * store the values by CassandraDB. Its because her do not recognize the  [KeyValue] objects.
	 */
	protected String relatedObjectsIdsJson;
	
	/**
	 * Constructor
	 */
	@SuppressWarnings(value="all")
	public BaseDTO(Class clazz){
		this.tableName = clazz.getName();
	}
	
	public void generateId(){
		Double d = null;
		char[] hex = new char[]{'a','b','c','d','e','f','0','9','1','8','2','7','3','6','4','5','0'};
		StringBuffer sid = new StringBuffer();
		while(id.length() < 24){
			d = 16 * Math.random();
			sid.append(hex[d.intValue()]);
		}
		id = Long.toHexString(new java.util.Date().getTime()) + sid.toString();
	}
	
	/**
	 * Make the reverse operation of Method populateRelatedObjectsIds().
	 * Convert the List of KeyValue [relatedObjectsIds] into a 
	 * KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON String value.
	 */
	public void populateRelatedObjectsIdsJson() throws CassandraDataException{
		if(null!=getRelatedObjectsIds()){
			StringBuffer json = new StringBuffer();
			String separator = "";
			for(KeyValue keyValue : getRelatedObjectsIds()){
				json.append(separator + keyValue.getKey() + "," + keyValue.getValue());
				separator = ";";
			}
			setRelatedObjectsIdsJson(json.toString());
		}
	}
	
	/** 
	 * Convert the KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON String value
	 * into a List of KeyValue [relatedObjectsIds] attribute.
	 */
	public void populateRelatedObjectsIds(){
		relatedObjectsIds = new ArrayList<KeyValue>();
		if(null!=relatedObjectsIdsJson && relatedObjectsIdsJson.trim().length() > 0){
			String[] objs = relatedObjectsIdsJson.split(";");
			for(String obj: objs){
				if(null!=obj && obj.trim().length() > 0){
					if(obj.indexOf(",") == -1){
						continue;
					}
					String[] kv = obj.split(",");
					relatedObjectsIds.add(new KeyValue(kv[0],kv[1]));
				}
			}
		}
	}

	/**
	 * @return relatedObjectsIds
	 */
	public List<KeyValue> getRelatedObjectsIds() {
		return relatedObjectsIds;
	}

	/**
	 * Set the relatedObjectsIds
	 * @param relatedObjectsIds
	 */
	public void setRelatedObjectsIds(List<KeyValue> relatedObjectsIds) {
		this.relatedObjectsIds = relatedObjectsIds;
	}

	/**
	 * @return relatedObjects
	 */
	public List<KeyValue> getRelatedObjects() {
		return relatedObjects;
	}

	/**
	 * Set the relatedObjects
	 * @param relatedObjects
	 */
	public void setRelatedObjects(List<KeyValue> relatedObjects) {
		this.relatedObjects = relatedObjects;
	}
	
	/**
	 * Add a associated object.
	 * Example A -> B
	 * [relatedObjects] can contain multiple classes of objects:
	 * A(this) -> B(other class),
	 * A(this) -> C(other class),
	 * A(this) -> D(other class),
	 */
	public boolean addRelatedObject(String id,BaseDTO dto){
		if(null!=dto && null!=id && id.trim().length() > 0){
			if(null==relatedObjectsIds){
				relatedObjectsIds = new ArrayList<KeyValue>();
			}
			boolean alreadyAssociated = false;
			for(KeyValue keyValue: relatedObjectsIds){
				if(keyValue.getKey().equals(dto.getClass().getName()) && keyValue.getValue().equals(id)){
					alreadyAssociated = true;
					break;
				}
			}
			if(!alreadyAssociated){
				return relatedObjectsIds.add(new KeyValue(dto.getClass().getName(),id));
			}
		}
		return false;
	}
	
	/**
	 * Remove a object association from this.
	 */
	public boolean removeRelatedObject(String id,BaseDTO dto){
		if(null!=dto && null!=relatedObjectsIds && relatedObjectsIds.size() > 0){
			if(null!=id && id.trim().length() > 0){
				for(KeyValue keyValue: relatedObjectsIds){
					if(keyValue.getKey().equals(dto.getClass().getName()) && keyValue.getValue().equals(id)){
						return relatedObjectsIds.remove(keyValue);
					}
				}
			}
		}
		return false;
	}
	
	/**
	 * Return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * Set the tableName
	 * @param tableName
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * Return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * Set the id
	 * @param id
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * Return relatedObjectsIdsJson
	 */
	public String getRelatedObjectsIdsJson() {
		return relatedObjectsIdsJson;
	}

	/**
	 * Set the relatedObjectsIdsJson
	 */
	public void setRelatedObjectsIdsJson(String relatedObjectsIdsJson) {
		this.relatedObjectsIdsJson = relatedObjectsIdsJson;
	}
	
	
}
