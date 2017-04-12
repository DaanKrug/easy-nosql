package krug.daan.easynosql.dynamodb.dto;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import krug.daan.easynosql.common.KeyValue;
import krug.daan.easynosql.dynamodb.exception.DynamoDataException;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;

/**
 * @author Daniel Augusto Krug
 * 
 * Class to be extended by DynamoDB persistent objects.
 * Encapsulate the data for BaseDAO handle on database access.
 */
public class BaseDTO {
	
	/**
	 * ID_ATTR_DESCRIPTOR
	 */
	public static final String ID_ATTR_DESCRIPTOR = "id";
	
	/**
	 * TABLE_NAME_ATTR_DESCRIPTOR
	 */
	public static final String TABLE_NAME_ATTR_DESCRIPTOR = "tableName";
	
	/**
	 * KEYVALUES_ATTR_RELATED_OBJECTS_IDS
	 */
	public static final String KEYVALUES_ATTR_RELATED_OBJECTS_IDS = "relatedObjectsIds";
	
	/**
	 * KEYVALUES_ATTR_RELATED_OBJECTS
	 */
	public static final String KEYVALUES_ATTR_RELATED_OBJECTS = "relatedObjects";
	
	/**
	 * KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON
	 */
	public static final String KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON = "relatedObjectsIdsJson";
	
	/**
	 * The table name mapping. 
	 */
	protected String tableName;
	
	/**
	 * Map the "id" attribute, auto generated value by DynamoDB mechanism to 
	 * DTO objects.
	 */
	protected String id;
	
	/**
	 * Map the related objects ids of others classes. Emulates the Object-Relational model.
	 */
	protected List<KeyValue> relatedObjectsIds;
	
	/**
	 * Map the related objects of others classes. Populated only when object is read from database,
	 * by a search on others tables according the [relatedObjectsIds] information
	 */
	protected List<KeyValue> relatedObjects;
	
	/**
	 * Map the List of KeyValue [relatedObjectsIds] to a unique String object, to be possible
	 * store the values by DynamoDB. Its because her do not recognize the  [KeyValue] objects.
	 */
	protected String relatedObjectsIdsJson;
	
	/**
	 * Constructor
	 */
	@SuppressWarnings(value="all")
	public BaseDTO(Class clazz){
		this.tableName = clazz.getName();
	}
	
	/**
	 * Make the reverse operation of Method populateRelatedObjectsIds().
	 * Convert the List of KeyValue [relatedObjectsIds] into a 
	 * KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON String value.
	 */
	public void populateRelatedObjectsIdsJson() throws DynamoDataException{
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
					String[] kv = obj.split(",");
					relatedObjectsIds.add(new KeyValue(kv[0],kv[1]));
				}
			}
		}
	}

	/**
	 * @return relatedObjectsIds
	 */
	@DynamoDBIgnore
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
	@DynamoDBIgnore
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
	 * Receives a list of KeyValue objects to set values in respective attributes
	 */
	public void setValues(List<KeyValue> keyValues) 
			throws InvocationTargetException, IllegalAccessException, DynamoDataException{
		try {
			Method[] methods = getClass().getDeclaredMethods();
			Field[] fields = getClass().getDeclaredFields();
			for(Field field: fields){
				for(KeyValue keyValue: keyValues){
					if(keyValue.getKey().equals(field.getName())){
						String methodName = "set" + field.getName().substring(0,1).toUpperCase() + field.getName().substring(1);
						Method method = null;
						for(Method m: methods){
							if(m.getName().equals(methodName)){
								method = m;
								break;
							}
						}
						if(null!=method){
							method.invoke(this, keyValue.getValue());
						}
					}
				}
			}
		} catch (Exception e) {
			throw new DynamoDataException(e);
		}
	}
	
	/**
	 * @return the "id"
	 * Should be override by classes that "extends" this.
	 */
	public String getId() throws DynamoDataException{
		throw new DynamoDataException("Method getId(): Should be override!");
	}

	/**
	 * Set the "id"
	 * @param id
	 * Should be override by classes that "extends" this.
	 */
	public void setId(String id) throws DynamoDataException{
		throw new DynamoDataException("Method setId(String id): Should be override!");
	}
	
	/**
	 * @return the table name
	 * Should be override by classes that "extends" this.
	 */
	public String getTableName() throws DynamoDataException{
		throw new DynamoDataException("Method getTableName(): Should be override!");
	}
	
	/**
	 * Set the tableName
	 * @param tableName
	 * Should be override by classes that "extends" this.
	 */
	public void setTableName(String tableName) throws DynamoDataException{
		throw new DynamoDataException("Method setTableName(String tableName): Should be override!");
	}
	
	/**
	 * @return relatedObjectsIdsJson
	 * Should be override by classes that "extends" this.
	 */
	public String getRelatedObjectsIdsJson() throws DynamoDataException{
		throw new DynamoDataException("Method getRelatedObjectsIdsJson(): Should be override!");
	}

	/**
	 * Sets the relatedObjectsIdsJson
	 * @param relatedObjectsIdsJson
	 * Should be override by classes that "extends" this.
	 */
	public void setRelatedObjectsIdsJson(String relatedObjectsIdsJson) throws DynamoDataException{
		throw new DynamoDataException("Method setRelatedObjectsIdsJson(String relatedObjectsIdsJson): Should be override!");
	}
}
