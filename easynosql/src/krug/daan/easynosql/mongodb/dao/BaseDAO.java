package krug.daan.easynosql.mongodb.dao;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import krug.daan.easynosql.common.KeyValue;
import krug.daan.easynosql.mongodb.config.MongoConfig;
import krug.daan.easynosql.mongodb.dto.BaseDTO;
import krug.daan.easynosql.mongodb.dto.RelationalIntegrityDTO;
import krug.daan.easynosql.mongodb.exception.MongoDataException;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * @author Daniel Augusto Krug
 *
 * Class to handle the basic CRUD operations on the MongoDB mechanism, 
 * making transparent the data access details to programmers.
 */
public class BaseDAO {
	
	/**
	 * TABLE_NAME_EMPTY_EXCEPTION
	 */
	private static final String TABLE_NAME_EMPTY_EXCEPTION = "<tableName> parameter couldnt be null or empty.";
	
	/**
	 * DELETE_ATTRIBUTES_EMPTY_EXCEPTION
	 */
	private static final String DELETE_ATTRIBUTES_EMPTY_EXCEPTION = "<searchParameters> parameter or a not null (and not empty) <BaseDTO> object should be passed.";
	
	/**
	 * SEARCH_PARAMETERS_EMPTY_EXCEPTION
	 */
	private static final String SEARCH_PARAMETERS_EMPTY_EXCEPTION = "<searchParameters> parameter couldnt be null or empty.";
	
	/**
	 * UPDATE_PARAMETERS_EMPTY_EXCEPTION
	 */
	private static final String UPDATE_PARAMETERS_EMPTY_EXCEPTION = "<updateConditions> parameter couldnt be null or empty.";
	
	/**
	 * OBJECT_ID_PARAMETER_EMPTY_EXCEPTION
	 */
	private static final String OBJECT_ID_PARAMETER_EMPTY_EXCEPTION = "<oid> parameter couldnt be null or empty.";
	
	/**
	 * OBJECT_ID_PARAMETER_EMPTY_EXCEPTION
	 */
	private static final String OBJECT_ID_PARAMETER_INVALID_EXCEPTION = "<oid> parameter should be a <String> or <ObjectId> type.";
	
	/**
	 * DUPLICATED_OBJECT_ID_EXCEPTION
	 */
	private static final String DUPLICATED_OBJECT_ID_EXCEPTION = "Duplicated object id: ";
	
	/**
	 * GHOST_OBJECT_ID_EXCEPTION
	 */
	private static final String GHOST_OBJECT_ID_EXCEPTION = "The Object dont exist or was deleted. Searched object id: ";
	
	/**
	 * RELATED_OBJECTS_ID_FIELD_JSON
	 */
	private static final String RELATED_OBJECTS_ID_FIELD_JSON = "relatedObjectsIdsJson";
	
	/**
	 * RELATED_OBJECTS_INTEGRITY_EXCEPTION
	 */
	private static final String RELATED_OBJECTS_INTEGRITY_EXCEPTION = "These Objects can not be deleted due to existing Relational Integrity: ";
	
	
	/**
	 * DELETED_RESULT_NO_MATCH_REFERENCES_TO_REMOVE_EXCEPTION
	 */
	private static final String DELETED_RESULT_NO_MATCH_REFERENCES_TO_REMOVE_EXCEPTION 
						= "Relational References cannot was removed: deleted result dont equals to number of relational references to remove! ";
	
	
	/**
	 * Mongo DB configuration Object
	 */
	private MongoConfig mgc;
	
	/**
	 * Singleton instance
	 */
	private static BaseDAO instance;
	
	/**
	 * Return the singleton instance
	 */
	public static BaseDAO getInstance(MongoConfig mgc){
		if(null==instance){
			instance = new BaseDAO(mgc);
		}
		return instance;
	}
	
	/**
	 * Constructor
	 */
	protected BaseDAO(MongoConfig mgc){
		this.mgc = mgc;
	}
	
	/**
	 * Creates a new Object.
	 */
	public void saveOrUpdate(BaseDTO dto) throws MongoDataException{
		if(null==dto.getTableName() || dto.getTableName().trim().length() == 0){
			throw new MongoDataException(TABLE_NAME_EMPTY_EXCEPTION);
		}
		try {
			if(null!=dto.getId() && dto.getId().toHexString().trim().length() > 0){
				update(dto);
				return;
			}
			MongoCollection<Document> table = mgc.getCollection(dto.getTableName());
			Document document = new Document();
			dto.generateKeyValues();
			List<KeyValue> keyValues = dto.getAllAttributeValues();
			for(KeyValue keyValue: keyValues){
				document.put(keyValue.getKey(),keyValue.getValue());
			}
			table.insertOne(document);
		} 
		catch (MongoDataException me){
			throw (me);
		}
		catch (Exception e) {
			throw new MongoDataException(e);
		}
	}
	
	/**
	 * Update values of attributes of a one specific Object.
	 * The values will be updated according the actual values of a object.
	 */
	public UpdateResult update(BaseDTO dto) 
			throws MongoDataException{
		try {
			List<KeyValue> updateParameters = new ArrayList<KeyValue>();
			updateParameters.add(new KeyValue("_id",dto.getId()));
			return update(dto,updateParameters);
		} 
		catch (MongoDataException me){
			throw (me);
		}
		catch (Exception e) {
			throw new MongoDataException(e);
		}
	}
	
	/**
	 * Update values of attributes of a one specific Object.
	 */
	public UpdateResult update(BaseDTO dto, List<KeyValue> updateConditions) 
			throws MongoDataException{
		if(null==dto.getTableName() || dto.getTableName().trim().length() == 0){
			throw new MongoDataException(TABLE_NAME_EMPTY_EXCEPTION);
		}
		if(null==updateConditions || updateConditions.size() == 0){
			throw new MongoDataException(UPDATE_PARAMETERS_EMPTY_EXCEPTION);
		}
		try {
			MongoCollection<Document> table = mgc.getCollection(dto.getTableName());
			BasicDBObject query = new BasicDBObject();
			for(KeyValue keyValue: updateConditions){
				query.put(keyValue.getKey(),keyValue.getValue());
			}
			Document document = new Document();
			dto.generateKeyValues();
			for(KeyValue keyValue: dto.getAllAttributeValues()){
				document.put(keyValue.getKey(),keyValue.getValue());
			}
			BasicDBObject set = new BasicDBObject("$set", document);
			UpdateResult ur =  table.updateOne(query,set);
			if(ur.getModifiedCount() > 0){
				if(!(dto instanceof RelationalIntegrityDTO)){
					RelationalIntegrityDAO.getInstance(mgc).updateRelationalIntegrityDTO(dto);
				}
			}
			return ur;
		} 
		catch (MongoDataException me){
			throw (me);
		}
		catch (Exception e) {
			throw new MongoDataException(e);
		}
	}
	
	/**
	 * Update all objects that matches [searchParameters] whit the [updateParameters] values.
	 */
	public UpdateResult updateAll(BaseDTO dto,List<KeyValue> searchParameters, List<KeyValue> updateParameters) 
			throws MongoDataException{
		if(null==dto.getTableName() || dto.getTableName().trim().length() == 0){
			throw new MongoDataException(TABLE_NAME_EMPTY_EXCEPTION);
		}
		if(null==searchParameters || searchParameters.size() == 0){
			throw new MongoDataException(SEARCH_PARAMETERS_EMPTY_EXCEPTION);
		}
		if(null==updateParameters || updateParameters.size() == 0){
			throw new MongoDataException(UPDATE_PARAMETERS_EMPTY_EXCEPTION);
		}
		try {
			MongoCollection<Document> table = mgc.getCollection(dto.getTableName());
			BasicDBObject updateDocument = new BasicDBObject();
			BasicDBObject updateObject = new BasicDBObject();
			for(KeyValue keyValue: updateParameters){
				updateObject.append(keyValue.getKey(),keyValue.getValue());
			}
			updateDocument.append("$set",updateObject);
			BasicDBObject searchObject = new BasicDBObject();
			for(KeyValue keyValue: searchParameters){
				searchObject.append(keyValue.getKey(),keyValue.getValue());
			}
			return table.updateMany(searchObject, updateDocument);
		} catch (Exception e) {
			throw new MongoDataException(e);
		}
	}
	
	/**
	 * Internal auxiliary Method to find() Method(s) uses.
	 */
	private FindIterable<Document> findIterable(BaseDTO dto,List<KeyValue> searchParameters) throws MongoDataException{
		if(null==dto.getTableName() || dto.getTableName().trim().length() == 0){
			throw new MongoDataException(TABLE_NAME_EMPTY_EXCEPTION);
		}
		try {
			BasicDBObject searchQuery = new BasicDBObject();
			if(null!=searchParameters && searchParameters.size() > 0){
				for(KeyValue keyValue: searchParameters){
					searchQuery.put(keyValue.getKey(),keyValue.getValue());
				}
			}else if(null!=dto.getAllAttributeValues() && dto.getAllAttributeValues().size() > 0){
				for(KeyValue keyValue: dto.getAllAttributeValues()){
					if(null!=keyValue.getValue()){
						searchQuery.put(keyValue.getKey(),keyValue.getValue());
					}
				}
			}
			MongoCollection<Document> table = mgc.getCollection(dto.getTableName());
			return table.find(searchQuery);
		} 
		catch (Exception e) {
			throw new MongoDataException(e);
		}
	}
	
	/**
	 * Return all objects that matches whit the [searchParameters] values.
	 * if [searchParameters>]is empty, then the [searchParameters] will be obtained by the 
	 * BaseDTO.getAllAttributeValues() Method.
	 */
	public Collection<BaseDTO> find(BaseDTO dto,List<KeyValue> searchParameters) 
			throws MongoDataException, InvocationTargetException, IllegalAccessException, InstantiationException{
		FindIterable<Document> fit = findIterable(dto, searchParameters);
		Collection<BaseDTO> dtos = new ArrayList<BaseDTO>();
		if(null!=fit){
			Iterator<Document> it = fit.iterator();
			Field[] BaseDTOFields = BaseDTO.class.getDeclaredFields();
			Method[] BaseDTOMethods = BaseDTO.class.getDeclaredMethods();
			Field[] fields = dto.getClass().getDeclaredFields();
			Method[] methods = dto.getClass().getDeclaredMethods();
			while(it.hasNext()){
				BaseDTO dtoSearch = dto.getClass().newInstance();
				Document doc = it.next();
				dtoSearch.setId(doc.getObjectId(BaseDTO.ID_ATTR_DESCRIPTOR));
				for(Field field: BaseDTOFields){
					if(field.getName().equals(BaseDTO.TABLE_NAME_ATTR_DESCRIPTOR)
							|| field.getName().equals(BaseDTO.KEYVALUES_ATTR_DESCRIPTOR)
							|| field.getName().equals(BaseDTO.ID_ATTR_DESCRIPTOR)
							|| field.getName().equals(BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS)
							|| field.getName().equals(BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS_IDS)
							){
						continue;
					}
					setFieldValueOnObject(doc, BaseDTOMethods, field, dtoSearch,true);
				}
				for(Field field: fields){
					setFieldValueOnObject(doc, methods, field, dtoSearch,true);
				}
				dtos.add(dtoSearch);
			}
		}
		return dtos;
	}
	
	/**
	 * Return a object from a table that macht the specific "_id"
	 */
	public BaseDTO findById(BaseDTO dto,Object oid, boolean initializeRelatedObjects) 
			throws MongoDataException, InvocationTargetException, IllegalAccessException, InstantiationException{
		if(null==oid){
			throw new MongoDataException(OBJECT_ID_PARAMETER_EMPTY_EXCEPTION);
		}
		ObjectId objectId = null;
		if(oid instanceof ObjectId){
			objectId = (ObjectId)oid;
		}else if(oid instanceof String){
			String soid = (String)oid;
			if(soid.trim().length() == 0){
				throw new MongoDataException(OBJECT_ID_PARAMETER_EMPTY_EXCEPTION);
			}
			objectId = new ObjectId(soid);
		}
		if(null==objectId){
			throw new MongoDataException(OBJECT_ID_PARAMETER_INVALID_EXCEPTION);
		}
		FindIterable<Document> fit = findIterable(dto, null);
		Collection<BaseDTO> dtos = new ArrayList<BaseDTO>();
		if(null!=fit){
			Iterator<Document> it = fit.iterator();
			Field[] BaseDTOFields = BaseDTO.class.getDeclaredFields();
			Method[] BaseDTOMethods = BaseDTO.class.getDeclaredMethods();
			Field[] fields = dto.getClass().getDeclaredFields();
			Method[] methods = dto.getClass().getDeclaredMethods();
			while(it.hasNext()){
				Document doc = it.next();
				ObjectId id = doc.getObjectId(BaseDTO.ID_ATTR_DESCRIPTOR);
				if(!(id.equals(objectId))){
					continue;
				}
				BaseDTO dtoSearch = dto.getClass().newInstance();
				dtoSearch.setId(id);
				for(Field field: BaseDTOFields){
					if(field.getName().equals(BaseDTO.TABLE_NAME_ATTR_DESCRIPTOR)
							|| field.getName().equals(BaseDTO.KEYVALUES_ATTR_DESCRIPTOR)
							|| field.getName().equals(BaseDTO.ID_ATTR_DESCRIPTOR)
							|| field.getName().equals(BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS)
							|| field.getName().equals(BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS_IDS)
							){
						continue;
					}
					setFieldValueOnObject(doc, BaseDTOMethods, field, dtoSearch, initializeRelatedObjects);
				}
				for(Field field: fields){
					setFieldValueOnObject(doc, methods, field, dtoSearch, initializeRelatedObjects);
				}
				dtos.add(dtoSearch);
			}
		}
		if(dtos.size() == 0){
			throw new MongoDataException(GHOST_OBJECT_ID_EXCEPTION + " " + objectId);
		}else if(dtos.size() > 1){
			throw new MongoDataException(DUPLICATED_OBJECT_ID_EXCEPTION + " " + objectId);
		}
		return dtos.iterator().next();
	}
	
	/**
	 * Set the fields values on a object from a Document object, by reflection.
	 */
	private void setFieldValueOnObject(Document doc, Method[] methods, Field field, BaseDTO dto, boolean initializeRelatedObjects) 
			throws InvocationTargetException, IllegalAccessException, InstantiationException{
		Object fieldValue = doc.get(field.getName());
		String methodName = "set" + field.getName().substring(0,1).toUpperCase() + field.getName().substring(1);
		Method method = null;
		for(Method m: methods){
			if(m.getName().equals(methodName)){
				method = m;
				break;
			}
		}
		if(null!=method){
			if(field.getName().equals(RELATED_OBJECTS_ID_FIELD_JSON)){
				method.invoke(dto, (String)fieldValue);
				dto.populateRelatedObjectsIds();
				if(initializeRelatedObjects){
					dto.setRelatedObjects(new ArrayList<KeyValue>());
					for(KeyValue keyValue: dto.getRelatedObjectsIds()){
						try {
							@SuppressWarnings(value="all")
							Class clazz = Class.forName(keyValue.getKey());
							BaseDTO relatedDTO = (BaseDTO)clazz.newInstance();
							BaseDTO dtoToAdd = findById(relatedDTO, keyValue.getValue(), false);
							dto.getRelatedObjects().add(new KeyValue(keyValue.getKey(), dtoToAdd));
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}else{
				method.invoke(dto,fieldValue);
			}
		}
	}
	
	/**
	 * Delete a object by her "_id" value
	 */
	public DeleteResult deleteById(BaseDTO dto) 
			throws MongoDataException{
		try {
			List<KeyValue> updateParameters = new ArrayList<KeyValue>();
			updateParameters.add(new KeyValue(BaseDTO.ID_ATTR_DESCRIPTOR,dto.getId()));
			return deleteAll(dto,updateParameters);
		} 
		catch (MongoDataException me){
			throw (me);
		}
		catch (Exception e) {
			throw new MongoDataException(e);
		}
	}
	
	/**
	 * Delete all objects that matches whit the [searchParameters] values.
	 * if [searchParameters] is empty, then the [searchParameters] will be obtained by the 
	 * BaseDTO.getAllAttributeValues() Method.
	 */
	public DeleteResult deleteAll(BaseDTO dto,List<KeyValue> searchParameters) throws MongoDataException{
		if(null==dto.getTableName() || dto.getTableName().trim().length() == 0){
			throw new MongoDataException(TABLE_NAME_EMPTY_EXCEPTION);
		}
		if( (null==searchParameters || searchParameters.size() == 0) 
				&& (null==dto.getAllAttributeValues() || dto.getAllAttributeValues().size() == 0)){
			throw new MongoDataException(DELETE_ATTRIBUTES_EMPTY_EXCEPTION);
		}
		try {
			if(null==searchParameters){
				searchParameters = new ArrayList<KeyValue>();
				dto.generateKeyValues();
				for(KeyValue keyValue: dto.getAllAttributeValues()){
					if(null!=keyValue.getValue()){
						searchParameters.add(keyValue);
					}
				}
			}
			FindIterable<Document> fit = findIterable(dto, searchParameters);
			Iterator<Document> it = fit.iterator();
			StringBuffer exclusionRestrictionsMsgs = new StringBuffer();
			List<KeyValue> relationalIntegritiesToRemove = new ArrayList<KeyValue>();
			if(!(dto instanceof RelationalIntegrityDTO)){
				while(it.hasNext()){
					Document doc = it.next();
					ObjectId id = doc.getObjectId(BaseDTO.ID_ATTR_DESCRIPTOR);
					BaseDTO dtoExclusionValidate = dto.getClass().newInstance();
					dtoExclusionValidate.setId(id);
					relationalIntegritiesToRemove.add(new KeyValue(dto.getClass().getName(),id.toHexString()));
					Collection<RelationalIntegrityDTO> ridtos = RelationalIntegrityDAO.getInstance(mgc).getLockedByRelationalIntegrity(dtoExclusionValidate);
					if(null!=ridtos && ridtos.size() > 0){
						exclusionRestrictionsMsgs.append("\nObject class: " + dto.getClass().getName() + " Object _id: " +  id.toHexString());
						exclusionRestrictionsMsgs.append(" whit relational restrictions on:");
						for(RelationalIntegrityDTO ridto: ridtos){
							exclusionRestrictionsMsgs.append("\n\t class: " + ridto.getOwnerTableName() + " _id: " + ridto.getOwnerId());
						}
					}
				}
				if(exclusionRestrictionsMsgs.length() > 0){
					throw new MongoDataException(RELATED_OBJECTS_INTEGRITY_EXCEPTION + exclusionRestrictionsMsgs.toString());
				}
			}
			BasicDBObject searchQuery = new BasicDBObject();
			for(KeyValue keyValue: searchParameters){
				searchQuery.put(keyValue.getKey(),keyValue.getValue());
			}
			MongoCollection<Document> table = mgc.getCollection(dto.getTableName());
			DeleteResult dr = table.deleteMany(searchQuery);
			if(!(dto instanceof RelationalIntegrityDTO)){
				if(dr.getDeletedCount() == relationalIntegritiesToRemove.size()){
					RelationalIntegrityDAO.getInstance(mgc).removeRelationalIntegrityDTO(relationalIntegritiesToRemove);
				}else{
					throw new MongoDataException(
							DELETED_RESULT_NO_MATCH_REFERENCES_TO_REMOVE_EXCEPTION 
							+ ("(" + dr.getDeletedCount() + " != " + relationalIntegritiesToRemove.size() + ")"));
				}
			}
			return dr;
		} 
		catch (MongoDataException me){
			throw (me);
		}
		catch (Exception e) {
			throw new MongoDataException(e);
		}
	}

}
