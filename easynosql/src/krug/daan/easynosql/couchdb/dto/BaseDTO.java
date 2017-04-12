package krug.daan.easynosql.couchdb.dto;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import krug.daan.easynosql.common.KeyValue;
import krug.daan.easynosql.couchdb.exception.CouchDataException;

/**
 * @author Daniel Augusto Krug
 * 
 * Class to be extended by CouchDB persistent objects.
 * Encapsulate the data for BaseDAO handle on database access.
 */
public class BaseDTO {
	
	/**
	 * TABLE_NAME_ATTR_DESCRIPTOR
	 */
	public static final String TABLE_NAME_ATTR_DESCRIPTOR = "tableName";
	
	/**
	 * KEYVALUES_ATTR_DESCRIPTOR
	 */
	private static final String KEYVALUES_ATTR_DESCRIPTOR = "attrs";
	
	/**
	 * KEYVALUES_ATTR_RELATED_OBJECTS_IDS
	 */
	private static final String KEYVALUES_ATTR_RELATED_OBJECTS_IDS = "relatedObjectsIds";
	
	/**
	 * KEYVALUES_ATTR_RELATED_OBJECTS
	 */
	private static final String KEYVALUES_ATTR_RELATED_OBJECTS = "relatedObjects";
	
	/**
	 * KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON
	 */
	private static final String KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON = "relatedObjectsIdsJson";
	
	/**
	 * INVALID_ATTR_MATCH_COMPARISON
	 */
	private static final String INVALID_ATTR_MATCH_COMPARISON = "Invalid attribute to match comparisons: ";
	
	/**
	 * List of KeyValue that represents the object handled by the BaseDAO class,
	 * to make possible the CRUD operations.
	 */
	protected List<KeyValue> attrs;
	
	/**
	 * Map the "id" attribute, auto generated value on constructor mechanism to 
	 * DTO objects
	 */
	protected String _id;
	
	/**
	 * Map the "id" attribute, auto generated value on constructor mechanism to 
	 * DTO objects
	 */
	protected String id;
	
	/**
	 * Map the "rev" attribute, auto generated value on constructor mechanism to 
	 * DTO objects
	 */
	protected String _rev;
	
	/**
	 * Map the "rev" attribute, auto generated value on constructor mechanism to 
	 * DTO objects
	 */
	protected String rev;
	
	/**
	 * Identify the class of objects to find functions.
	 */
	protected String tableName;
	
	/**
	 * Identify the class of objects to find functions.
	 */
	protected String tableNameControl;
	
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
	 * store the values by CouchDB. Its because her do not recognize the  [KeyValue] objects.
	 */
	protected String relatedObjectsIdsJson;
	
	/**
	 * Constructor
	 */
	@SuppressWarnings(value="all")
	public BaseDTO(Class clazz){
		this.attrs = null;
		this.tableName = clazz.getName();
	}
	
	/**
	 * Create the mapping from object attributes to a list of KeyValue objects
	 */
	public void generateKeyValues() throws InvocationTargetException, IllegalAccessException{
		this.attrs = new ArrayList<KeyValue>();
		Method[] BaseDTOMethods = BaseDTO.class.getDeclaredMethods();
		Field[] BaseDTOFields = BaseDTO.class.getDeclaredFields();
		for(Field field: BaseDTOFields){
			if(field.getName().equals(BaseDTO.KEYVALUES_ATTR_DESCRIPTOR)
					|| field.getName().equals(BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS)
					|| field.getName().equals(BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS_IDS)
					){
				continue;
			}
			if(field.getName().equals(BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON)){
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
	 * Verify that the object attributes values matches or not whit desired values
	 */
	public boolean matchKeyValues(List<KeyValue> keyValues) 
			throws InvocationTargetException, IllegalAccessException, CouchDataException{
		int matches = 0;
		
		Method[] baseDTOmethods = BaseDTO.class.getDeclaredMethods();
		Field[] baseDTOfields = BaseDTO.class.getDeclaredFields();
		for(Field field: baseDTOfields){
			for(KeyValue keyValue: keyValues){
				if(keyValue.getKey().equals(field.getName())){
					String methodName = "get" + field.getName().substring(0,1).toUpperCase() + field.getName().substring(1);
					Method method = null;
					for(Method m: baseDTOmethods){
						if(m.getName().equals(methodName)){
							method = m;
							break;
						}
					}
					if(null!=method){
						@SuppressWarnings(value="all")
						Object obj = method.invoke(this, null);
						if(field.getName().equals(KEYVALUES_ATTR_RELATED_OBJECTS_IDS)
								|| field.getName().equals(KEYVALUES_ATTR_RELATED_OBJECTS)
								|| field.getName().equals(KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON)
								|| field.getName().equals(KEYVALUES_ATTR_DESCRIPTOR)
								){
							throw new CouchDataException(INVALID_ATTR_MATCH_COMPARISON + " " + field.getName());
						}else if(null!=obj){
							if(obj.toString().equals(keyValue.getValue().toString())){
								matches ++;
							}
						}
					}
				}
			}
		}
		
		Method[] methods = getClass().getDeclaredMethods();
		Field[] fields = getClass().getDeclaredFields();
		for(Field field: fields){
			for(KeyValue keyValue: keyValues){
				if(keyValue.getKey().equals(field.getName())){
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
						if(field.getName().equals(KEYVALUES_ATTR_RELATED_OBJECTS_IDS)
								|| field.getName().equals(KEYVALUES_ATTR_RELATED_OBJECTS)
								|| field.getName().equals(KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON)
								|| field.getName().equals(KEYVALUES_ATTR_DESCRIPTOR)
								){
							throw new CouchDataException(INVALID_ATTR_MATCH_COMPARISON + " " + field.getName());
						}else if(null!=obj){
							if(obj.toString().equals(keyValue.getValue().toString())){
								matches ++;
							}
						}
					}
				}
			}
		}
		return (matches == keyValues.size());
	}
	
	/**
	 * Receives a list of KeyValue objects to set values in respective attributes
	 */
	public void setValues(List<KeyValue> keyValues) 
			throws InvocationTargetException, IllegalAccessException, CouchDataException{
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
			throw new CouchDataException(e);
		}
	}
	
	/**
	 * @return the "id"
	 */
	public String getId() {
		return (null!=_id ? _id : id);
	}

	/**
	 * Set the "id"
	 * @param id
	 */
	public void setId(String id) {
		this._id = id;
		this.id = id;
	}
	
	/**
	 * @return rev
	 */
	public String getRev() {
		return (null!=rev ? rev : _rev);
	}

	/**
	 * Set the rev
	 * @param rev
	 */
	public void setRev(String rev) {
		this.rev = rev;
		this._rev = rev;
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
	
	/**
	 * @return tableName
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
	 * @return tableNameControl
	 */
	public String getTableNameControl() {
		return tableNameControl;
	}

	/**
	 * Set the tableNameControl
	 * @param tableNameControl
	 */
	public void setTableNameControl(String tableNameControl) {
		this.tableNameControl = tableNameControl;
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
}
