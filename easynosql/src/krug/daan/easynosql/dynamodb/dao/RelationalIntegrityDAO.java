package krug.daan.easynosql.dynamodb.dao;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import krug.daan.easynosql.common.KeyValue;
import krug.daan.easynosql.dynamodb.aws.AwsKeyValue;
import krug.daan.easynosql.dynamodb.config.DynamoConfig;
import krug.daan.easynosql.dynamodb.dto.BaseDTO;
import krug.daan.easynosql.dynamodb.dto.RelationalIntegrityDTO;
import krug.daan.easynosql.dynamodb.exception.DynamoDataException;
import krug.daan.easynosql.dynamodb.type.ComparisonType;
import krug.daan.easynosql.dynamodb.type.DataType;

/**
 * @author Daniel Augusto Krug
 * 
 * Class to handle the persistence of relational references between two objects
 */
class RelationalIntegrityDAO extends BaseDAO{
	
	/**
	 * Singleton instance
	 */
	private static RelationalIntegrityDAO instance;
	
	/**
	 * Get the Singleton instance
	 */
	public static RelationalIntegrityDAO getInstance(DynamoConfig dnc){
		if(null==instance){
			instance = new RelationalIntegrityDAO(dnc);
		}
		return instance;
	}
	
	/**
	 * Constructor
	 */
	protected RelationalIntegrityDAO(DynamoConfig dnc){
		super(dnc);
	}
	
	/**
	 * Updates the [RelationalIntegrityDTO] associated whit the
	 * [dto]. RelationalIntegrityDTO are used to control if a 
	 * [BaseDTO] can be deleted or not. If a BaseDTO 'DTO'  is
	 * present in one or more RelationalIntegrityDTO object(s) 
	 * that the owner is other(s) BaseDTO(s) object(s), then
	 * exists a Relational Integrity between these Objects 
	 * and 'DTO' will not be deleted to maintain
	 * the integrity of references.
	 */
	public void updateRelationalIntegrityDTO(BaseDTO dto) 
			throws InstantiationException, IllegalAccessException,InvocationTargetException, DynamoDataException{
		List<AwsKeyValue> searchParameters = new ArrayList<AwsKeyValue>();
		searchParameters.add(new AwsKeyValue(RelationalIntegrityDTO.OWNER_ID,dto.getId(),DataType.STRING,ComparisonType.EQUAL));
		searchParameters.add(new AwsKeyValue(RelationalIntegrityDTO.OWNER_TABLE_NAME,dto.getClass().getName(),DataType.STRING,ComparisonType.EQUAL));
		deleteAll(new RelationalIntegrityDTO(), searchParameters);
		if(null!=dto.getRelatedObjectsIds() && dto.getRelatedObjectsIds().size() > 0){
			List<RelationalIntegrityDTO> newRelations = new ArrayList<RelationalIntegrityDTO>();
			for(KeyValue keyValue: dto.getRelatedObjectsIds()){
				RelationalIntegrityDTO ridto = new RelationalIntegrityDTO();
				ridto.setOwnerId(dto.getId());
				ridto.setOwnerTableName(dto.getClass().getName());
				ridto.setRelatedTableName(keyValue.getKey());
				ridto.setRelatedId((String)keyValue.getValue());
				newRelations.add(ridto);
			}
			if(newRelations.size() > 0){
				for(RelationalIntegrityDTO ridto: newRelations){
					saveOrUpdate(ridto);
				}
			}
		}
	}
	
	/**
	 * Remove the [relationalIntegrityDTO] objects that machtes whit [relationalIntegrities]
	 */
	public void removeRelationalIntegrityDTO(List<KeyValue> relationalIntegrities) throws DynamoDataException{
		for(KeyValue keyValue: relationalIntegrities){
			List<AwsKeyValue> searchParameters = new ArrayList<AwsKeyValue>();
			searchParameters.add(new AwsKeyValue(RelationalIntegrityDTO.OWNER_ID,keyValue.getValue(),DataType.STRING,ComparisonType.EQUAL));
			searchParameters.add(new AwsKeyValue(RelationalIntegrityDTO.OWNER_TABLE_NAME,keyValue.getKey(),DataType.STRING,ComparisonType.EQUAL));
			deleteAll(new RelationalIntegrityDTO(), searchParameters);
		}
	}
	
	/**
	 * Verify if a object can be deleted by Relational Integrity Rule,
	 * as explained farther up in [updateRelationalIntegrityDTO] method.
	 */
	public Collection<RelationalIntegrityDTO> getLockedByRelationalIntegrity(BaseDTO dto) 
			throws InstantiationException, IllegalAccessException, InvocationTargetException, DynamoDataException{
		List<AwsKeyValue> searchParameters = new ArrayList<AwsKeyValue>();
		searchParameters.add(new AwsKeyValue(RelationalIntegrityDTO.RELATED_ID,dto.getId(),DataType.STRING,ComparisonType.EQUAL));
		searchParameters.add(new AwsKeyValue(RelationalIntegrityDTO.RELATED_TABLE_NAME,dto.getClass().getName(),DataType.STRING,ComparisonType.EQUAL));
		Collection<BaseDTO> dtos =  findAll(new RelationalIntegrityDTO(), searchParameters, -1, false,false);
		Collection<RelationalIntegrityDTO> ridtos = new ArrayList<RelationalIntegrityDTO>();
		for(BaseDTO dto2 : dtos){
			ridtos.add((RelationalIntegrityDTO)dto2);
		}
		return ridtos;
	}

}
