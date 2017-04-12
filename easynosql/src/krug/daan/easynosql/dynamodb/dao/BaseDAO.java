package krug.daan.easynosql.dynamodb.dao;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import krug.daan.easynosql.common.KeyValue;
import krug.daan.easynosql.dynamodb.dao.RelationalIntegrityDAO;
import krug.daan.easynosql.dynamodb.aws.AwsKeyValue;
import krug.daan.easynosql.dynamodb.config.DynamoConfig;
import krug.daan.easynosql.dynamodb.dto.BaseDTO;
import krug.daan.easynosql.dynamodb.dto.RelationalIntegrityDTO;
import krug.daan.easynosql.dynamodb.exception.DynamoDataException;
import krug.daan.easynosql.dynamodb.type.ComparisonType;
import krug.daan.easynosql.dynamodb.type.DataType;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * @author Daniel Augusto Krug
 *
 * Class to handle BaseDTO objects storage on DynamoDB
 */
public class BaseDAO {
	
	/**
	 * RELATED_OBJECTS_INTEGRITY_EXCEPTION
	 */
	private static final String RELATED_OBJECTS_INTEGRITY_EXCEPTION = "These Objects can not be deleted due to existing Relational Integrity: ";
	
	/**
	 * OBJECT_ID_PARAMETER_EMPTY_EXCEPTION
	 */
	private static final String OBJECT_ID_PARAMETER_EMPTY_EXCEPTION = "Object getId() method couldnt return null or empty.";
	
	/**
	 * GHOST_OBJECT_ID_EXCEPTION
	 */
	private static final String GHOST_OBJECT_ID_EXCEPTION = "The Object dont exist or was deleted. Searched object id: ";
	
	/**
	 * DynamoConfig object
	 */
	private DynamoConfig dnc;
	
	/**
	 * AwsTableDAO object
	 */
	private AwsTableDAO awsDAO;
	
	/**
	 * Singleton instance
	 */
	private static BaseDAO instance;
	
	/**
	 * Return the singleton instance
	 */
	public static BaseDAO getInstance(DynamoConfig dnc){
		if(null==instance){
			instance = new BaseDAO(dnc);
		}
		return instance;
	}
	
	/**
	 * Constructor
	 */
	protected BaseDAO(DynamoConfig dnc){
		this.dnc = dnc;
		this.awsDAO = AwsTableDAO.getInstance(dnc);
	}
	
	/**
	 * Provides a AwsTableDAO public access
	 */
	public AwsTableDAO getAwsTableDAO(){
		return awsDAO;
	}
	
	/**
	 * Save or update a object
	 */
	public void saveOrUpdate(BaseDTO dto) throws DynamoDataException{
		try {
			if(!awsDAO.tableExist(dto.getTableName())){
				awsDAO.createTable(dto);
			}
			if(!(dto instanceof RelationalIntegrityDTO)){
				dto.populateRelatedObjectsIdsJson();
			}
			dnc.getMapper().save(dto);
			if(null!=dto.getId() && dto.getId().trim().length() > 0){
				if(!(dto instanceof RelationalIntegrityDTO)){
					RelationalIntegrityDAO.getInstance(dnc).updateRelationalIntegrityDTO(dto);
				}
			}
		} 
		catch (DynamoDataException de) {
			throw de;
		}
		catch (Exception e) {
			throw new DynamoDataException(e);
		}
	}
	
	/**
	 * Initialize the related objects on RelationalIntegrity objects
	 */
	private void initializeRelatedObjects(BaseDTO dto) throws IllegalAccessException, 
												InstantiationException, ClassNotFoundException, DynamoDataException{
		dto.setRelatedObjects(new ArrayList<KeyValue>());
		for(KeyValue keyValue: dto.getRelatedObjectsIds()){
			BaseDTO dtoSearch = (BaseDTO)Class.forName((String)keyValue.getKey()).newInstance();
			List<AwsKeyValue> searchParameters = new ArrayList<AwsKeyValue>();
			AwsKeyValue awskv = new AwsKeyValue(BaseDTO.ID_ATTR_DESCRIPTOR,
					(String)keyValue.getValue(),DataType.STRING,ComparisonType.EQUAL);
			searchParameters.add(awskv);
			BaseDTO dtoResult = find(dtoSearch,searchParameters,false, true);
			if(null!=dtoResult){
				dto.getRelatedObjects().add(new KeyValue((String)keyValue.getKey(),dtoResult));
			}
		}
	}
	
	/**
	 * Delete a object
	 */
	public void delete(BaseDTO dto) throws DynamoDataException{
		try {
			if(!awsDAO.tableExist(dto.getTableName())){
				throw new DynamoDataException("Table " + dto.getTableName() + " dont exists!");
			}
			if(null==dto.getId() || dto.getId().trim().length() == 0){
				throw new DynamoDataException(OBJECT_ID_PARAMETER_EMPTY_EXCEPTION);
			}
			if(!(dto instanceof RelationalIntegrityDTO)){
				StringBuffer exclusionRestrictionsMsgs = new StringBuffer();
				Collection<RelationalIntegrityDTO> ridtos = RelationalIntegrityDAO.getInstance(dnc).getLockedByRelationalIntegrity(dto);
				if(null!=ridtos && ridtos.size() > 0){
					exclusionRestrictionsMsgs.append("\nObject class: " + dto.getClass().getName() + " Object _id: " +  dto.getId());
					exclusionRestrictionsMsgs.append(" whit relational restrictions on:");
					for(RelationalIntegrityDTO ridto: ridtos){
						exclusionRestrictionsMsgs.append("\n\t class: " + ridto.getOwnerTableName() + " _id: " + ridto.getOwnerId());
					}
				}
				if(exclusionRestrictionsMsgs.length() > 0){
					throw new DynamoDataException(RELATED_OBJECTS_INTEGRITY_EXCEPTION + exclusionRestrictionsMsgs.toString());
				}
			}
			dnc.getMapper().delete(dto);
		} 
		catch (DynamoDataException de) {
			throw de;
		}
		catch (Exception e) {
			throw new DynamoDataException(e);
		}
	}
	
	/**
	 * Delete all objects that matches the search param values
	 */
	@SuppressWarnings(value="all")
	public long deleteAll(BaseDTO dto, List<AwsKeyValue> searchParameters) throws DynamoDataException{
		long deletedObjects = 0;
		List<BaseDTO> dtos = findAll(dto, searchParameters, -1, false, false);
		for(BaseDTO dto2: dtos){
			delete(dto2);
			deletedObjects ++;
		}
		return deletedObjects;
	}
	
	/**
	 * Update all objects that matches the search param values whit the update param values
	 */
	@SuppressWarnings(value="all")
	public long updateAll(BaseDTO dto, List<AwsKeyValue> searchParameters,List<KeyValue> updateParameters) 
											throws IllegalAccessException, InvocationTargetException ,DynamoDataException{
		long updatedObjects = 0;
		List<BaseDTO> dtos = findAll(dto, searchParameters, -1, false, false);
		for(BaseDTO dto2: dtos){
			dto2.setValues(updateParameters);
			saveOrUpdate(dto2);
			updatedObjects ++;
		}
		return updatedObjects;
	}
	
	/**
	 * Return a object from a table that match the specific "id" valu
	 */
	public BaseDTO findById(BaseDTO dto, boolean initializeRelatedObjects) 
			throws DynamoDataException, InvocationTargetException, IllegalAccessException, InstantiationException, ClassNotFoundException{
		if(null==dto.getId() || dto.getId().trim().length() == 0){
			throw new DynamoDataException(OBJECT_ID_PARAMETER_EMPTY_EXCEPTION);
		}
		List<AwsKeyValue> searchParameters = new ArrayList<AwsKeyValue>();
		searchParameters.add(new AwsKeyValue("id", dto.getId(),DataType.STRING,ComparisonType.EQUAL));
		List<BaseDTO> dtos = findAll(dto, searchParameters, 1, false, true);
		if(null==dtos || dtos.size() == 0){
			throw new DynamoDataException(GHOST_OBJECT_ID_EXCEPTION + " " + dto.getId());
		}
		BaseDTO dto2 = dtos.get(0);
		dto2.populateRelatedObjectsIds();
		if(initializeRelatedObjects){
			initializeRelatedObjects(dto2);
		}
		return dto2;
	}
	
	/**
	 * Search a object list
	 */
	public BaseDTO find(BaseDTO dto,List<AwsKeyValue> searchParameters, boolean initializeRelatedObjects, boolean filterByKeyExpression) throws DynamoDataException{
		try {
			if(!awsDAO.tableExist(dto.getTableName())){
				throw new DynamoDataException("Table " + dto.getTableName() + " dont exists!");
			}
			if(null!=searchParameters && searchParameters.size() > 0){
				List<BaseDTO> dtos = findAll(dto, searchParameters, 1, false, filterByKeyExpression);
				if(dtos.size() > 0){
					BaseDTO dto2 = dtos.get(0);
					dto2.populateRelatedObjectsIds();
					if(initializeRelatedObjects){
						initializeRelatedObjects(dto2);
					}
					return dto2;
				}
				return null;
			}else{
				return dnc.getMapper().load(dto);
			}
		} 
		catch (DynamoDataException de) {
			throw de;
		}
		catch (Exception e) {
			throw new DynamoDataException(e);
		}
	}
	
	/** 
	 * Search a object list
	 */
	@SuppressWarnings(value="all")
	public List<BaseDTO> findAll(BaseDTO dto, boolean initializeRelatedObjects) throws DynamoDataException{
		return findAll(dto, null, -1, initializeRelatedObjects, false);
	}
	
	/**
	 * Search a object list
	 */
	@SuppressWarnings(value="all")
	public List<BaseDTO> findAll(BaseDTO dto, List<AwsKeyValue> searchParameters, Integer limit, 
									boolean initializeRelatedObjects, boolean isKeyExpression) throws DynamoDataException{
		try {
			if(!awsDAO.tableExist(dto.getTableName())){
				if(dto instanceof RelationalIntegrityDTO){
					return new ArrayList<BaseDTO>();
				}
				throw new DynamoDataException("Table " + dto.getTableName() + " dont exists!");
			}
			Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
			StringBuffer conditionExpression = new StringBuffer();
			if(null!=searchParameters && searchParameters.size() > 0){
				int count = 0;
				String and = "";
				for(AwsKeyValue keyValue: searchParameters){
					String keyParam = ":val" + count;
					eav.put(keyParam,keyValue.generateAtributeValue());
					conditionExpression.append(and + keyValue.generateCondition(keyParam));
					count ++;
					and = " and ";
				}
			}else{
				// where tableName = 'tableName'
				// tableName = :val0
				String keyParam = ":val0";
				AttributeValue av = new AttributeValue();
				av.setS(dto.getTableName());
				eav.put(keyParam,av);
				conditionExpression.append(BaseDTO.TABLE_NAME_ATTR_DESCRIPTOR + " = " +  keyParam);
			}
			List<BaseDTO> dtos = null;
			Class clazz = dto.getClass();
			if(isKeyExpression){
				DynamoDBQueryExpression<BaseDTO> queryExpression = new DynamoDBQueryExpression<BaseDTO>();
				queryExpression.setKeyConditionExpression(conditionExpression.toString());
				queryExpression.setExpressionAttributeValues(eav);
				if(null!=limit && limit > 0){
					queryExpression.setLimit(limit);
				}
				dtos = dnc.getMapper().query(clazz, queryExpression);
			}else{
				DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
				scanExpression.setFilterExpression(conditionExpression.toString());
				scanExpression.setExpressionAttributeValues(eav);
				if(null!=limit && limit > 0){
					scanExpression.setLimit(limit);
				}
				dtos = dnc.getMapper().scan(clazz, scanExpression);
			}
			if(null==dtos){
				dtos = new ArrayList<BaseDTO>();
			}else if(initializeRelatedObjects){
				for(BaseDTO dto2 : dtos){
					dto2.populateRelatedObjectsIds();
					initializeRelatedObjects(dto2);
				}
			}
			return dtos;
		} 
		catch (DynamoDataException de) {
			throw de;
		}
		catch (Exception e) {
			throw new DynamoDataException(e);
		}
	}
	
	

}
