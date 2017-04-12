package krug.daan.easynosql.mongodb.dto;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import krug.daan.easynosql.common.KeyValue;

import org.bson.types.ObjectId;

/**
 * @author Daniel Augusto Krug
 * 
 * Class to be extended by MongoDB persistent objects.
 * Encapsulate the data for BaseDAO handle on database access.
 */
public class BaseDTO {
	
	/**
	 * ID_ATTR_DESCRIPTOR
	 */
	public static final String ID_ATTR_DESCRIPTOR = "_id";
	
	/**
	 * TABLE_NAME_ATTR_DESCRIPTOR
	 */
	public static final String TABLE_NAME_ATTR_DESCRIPTOR = "tableName";
	
	/**
	 * KEYVALUES_ATTR_DESCRIPTOR
	 */
	public static final String KEYVALUES_ATTR_DESCRIPTOR = "attrs";
	
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
	 * The table name mapping. A object of a class [User] 
	 * in a package "abc.cde.fgh" will be stored in a table named
	 * [abc.cde.fgh.User]
	 */
	protected String tableName;
	
	/**
	 * List of KeyValue that represents the object handled by the BaseDAO class,
	 * to make possible the CRUD operations.
	 */
	protected List<KeyValue> attrs;
	
	/**
	 * Map the "_id" attribute, auto generated value by MongoDB mechanism to 
	 * DTO objects
	 */
	protected ObjectId _id;
	
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
	 * store the values by MongoDB. Its because her do not recognize the  [KeyValue] objects.
	 */
	protected String relatedObjectsIdsJson;
	
	/**
	 * Constructor
	 */
	@SuppressWarnings(value="all")
	public BaseDTO(Class clazz){
		this.tableName = clazz.getName();
		this.attrs = null;
	}
	
	/**
	 * @return the mapping from object attributes to a list of KeyValue objects
	 */
	public List<KeyValue> getAllAttributeValues(){
		return attrs;
	}
	
	/**
	 * Set the mapping from object attributes to a list of KeyValue objects
	 */
	public void setAllAttributeValues(List<KeyValue> keyValues){
		this.attrs = keyValues;
	}
	
	/**
	 * Create the mapping from object attributes to a list of KeyValue objects
	 */
	public void generateKeyValues() throws InvocationTargetException, IllegalAccessException{
		this.attrs = new ArrayList<KeyValue>();
		Method[] BaseDTOMethods = BaseDTO.class.getDeclaredMethods();
		Field[] BaseDTOFields = BaseDTO.class.getDeclaredFields();
		for(Field field: BaseDTOFields){
			if(field.getName().equals(BaseDTO.TABLE_NAME_ATTR_DESCRIPTOR)
					|| field.getName().equals(BaseDTO.KEYVALUES_ATTR_DESCRIPTOR)
					|| field.getName().equals(BaseDTO.ID_ATTR_DESCRIPTOR)
					|| field.getName().equals(BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS)
					|| field.getName().equals(BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS_IDS)
					){
				continue;
			}
			if(field.getName().equals(BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON)){
				if(null!=getRelatedObjectsIds()){
					String json = "";
					String separator = "";
					for(KeyValue keyValue : getRelatedObjectsIds()){
						json += separator + keyValue.getKey() + "," + keyValue.getValue();
						separator = ";";
					}
					setRelatedObjectsIdsJson(json);
				}
			}
			String methodName = "get" + field.getName().substring(0,1).toUpperCase() + field.getName().substring(1);
			Method method = null;
			for(Method m: BaseDTOMethods){
				if(m.getName().equals(methodName)){
					method = m;
					break;
				}
			}
			if(null!=method){
				@SuppressWarnings(value="all")
				Object obj = method.invoke(this, null);
				this.attrs.add(new KeyValue(field.getName(),obj));
			}
		}
		Method[] methods = getClass().getDeclaredMethods();
		Field[] fields = getClass().getDeclaredFields();
		for(Field field: fields){
			String methodName = "get" + field.getName().substring(0,1).toUpperCase() + field.getName().substring(1);
			Method method = null;
			for(Method m: methods){
				if(m.getName().equals(methodName)){
					method = m;
					break;
				}
			}
			if(null!=method){
				@SuppressWarnings(value="all")
				Object obj = method.invoke(this, null);
				this.attrs.add(new KeyValue(field.getName(),obj));
			}
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
	 * @return the table name
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @return the "_id"
	 */
	public ObjectId getId() {
		return _id;
	}

	/**
	 * Set the "_id"
	 * @param _id
	 */
	public void setId(ObjectId _id) {
		this._id = _id;
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
	public boolean addRelatedObject(Object oid,BaseDTO dto){
		ObjectId objectId = null;
		if(oid instanceof ObjectId){
			objectId = (ObjectId)oid;
		}else if(oid instanceof String){
			String soid = (String)oid;
			if(soid.trim().length() > 0){
				objectId = new ObjectId(soid);
			}
		}
		if(null!=dto && null!=objectId){
			if(null==relatedObjectsIds){
				relatedObjectsIds = new ArrayList<KeyValue>();
			}
			boolean alreadyAssociated = false;
			for(KeyValue keyValue: relatedObjectsIds){
				if(keyValue.getKey().equals(dto.getClass().getName()) && keyValue.getValue().equals(objectId.toHexString())){
					alreadyAssociated = true;
					break;
				}
			}
			if(!alreadyAssociated){
				return relatedObjectsIds.add(new KeyValue(dto.getClass().getName(),objectId.toHexString()));
			}
		}
		return false;
	}
	
	/**
	 * Remove a object association from this.
	 */
	public boolean removeRelatedObject(Object oid,BaseDTO dto){
		if(null!=dto && null!=relatedObjectsIds && relatedObjectsIds.size() > 0){
			String hexadecimalId = null;
			if(oid instanceof ObjectId){
				hexadecimalId = ((ObjectId)oid).toHexString();
			}else if(oid instanceof String){
				hexadecimalId = (String)oid;
			}
			if(null!=hexadecimalId && hexadecimalId.trim().length() > 0){
				for(KeyValue keyValue: relatedObjectsIds){
					if(keyValue.getKey().equals(dto.getClass().getName()) && keyValue.getValue().equals(hexadecimalId)){
						return relatedObjectsIds.remove(keyValue);
					}
				}
			}
		}
		return false;
	}

	/**
	 * @return relatedObjectsIdsJson
	 */
	public String getRelatedObjectsIdsJson() {
		return relatedObjectsIdsJson;
	}

	/**
	 * Sets the relatedObjectsIdsJson
	 * @param relatedObjectsIdsJson
	 */
	public void setRelatedObjectsIdsJson(String relatedObjectsIdsJson) {
		this.relatedObjectsIdsJson = relatedObjectsIdsJson;
	}
}
