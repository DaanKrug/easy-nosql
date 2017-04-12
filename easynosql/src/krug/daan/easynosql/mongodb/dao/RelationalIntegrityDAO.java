package krug.daan.easynosql.mongodb.dao;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import krug.daan.easynosql.common.KeyValue;
import krug.daan.easynosql.mongodb.config.MongoConfig;
import krug.daan.easynosql.mongodb.dto.BaseDTO;
import krug.daan.easynosql.mongodb.dto.RelationalIntegrityDTO;
import krug.daan.easynosql.mongodb.exception.MongoDataException;

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
	 * Return the Singleton instance
	 */
	public static RelationalIntegrityDAO getInstance(MongoConfig mgc){
		if(null==instance){
			instance = new RelationalIntegrityDAO(mgc);
		}
		return instance;
	}
	
	/**
	 * Constructor
	 */
	protected RelationalIntegrityDAO(MongoConfig mgc){
		super(mgc);
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
			throws InstantiationException, IllegalAccessException,InvocationTargetException, MongoDataException{
		List<KeyValue> searchParameters = new ArrayList<KeyValue>();
		searchParameters.add(new KeyValue(RelationalIntegrityDTO.OWNER_ID,dto.getId().toHexString()));
		searchParameters.add(new KeyValue(RelationalIntegrityDTO.OWNER_TABLE_NAME,dto.getTableName()));
		deleteAll(new RelationalIntegrityDTO(), searchParameters);
		if(null!=dto.getRelatedObjectsIds() && dto.getRelatedObjectsIds().size() > 0){
			List<RelationalIntegrityDTO> newRelations = new ArrayList<RelationalIntegrityDTO>();
			for(KeyValue keyValue: dto.getRelatedObjectsIds()){
				RelationalIntegrityDTO ridto = new RelationalIntegrityDTO();
				ridto.setOwnerId(dto.getId().toHexString());
				ridto.setOwnerTableName(dto.getTableName());
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
	public void removeRelationalIntegrityDTO(List<KeyValue> relationalIntegrities) throws MongoDataException{
		for(KeyValue keyValue: relationalIntegrities){
			List<KeyValue> searchParameters = new ArrayList<KeyValue>();
			searchParameters.add(new KeyValue(RelationalIntegrityDTO.OWNER_ID,keyValue.getValue()));
			searchParameters.add(new KeyValue(RelationalIntegrityDTO.OWNER_TABLE_NAME,keyValue.getKey()));
			deleteAll(new RelationalIntegrityDTO(), searchParameters);
		}
	}
	
	/**
	 * Verify if a object can be deleted by Relational Integrity Rule,
	 * as explained farther up in [updateRelationalIntegrityDTO] method.
	 */
	public Collection<RelationalIntegrityDTO> getLockedByRelationalIntegrity(BaseDTO dto) 
			throws InstantiationException, IllegalAccessException, InvocationTargetException, MongoDataException{
		List<KeyValue> searchParameters = new ArrayList<KeyValue>();
		searchParameters.add(new KeyValue(RelationalIntegrityDTO.RELATED_ID,dto.getId().toHexString()));
		searchParameters.add(new KeyValue(RelationalIntegrityDTO.RELATED_TABLE_NAME,dto.getTableName()));
		Collection<BaseDTO> dtos =  find(new RelationalIntegrityDTO(), searchParameters);
		Collection<RelationalIntegrityDTO> ridtos = new ArrayList<RelationalIntegrityDTO>();
		for(BaseDTO dto2 : dtos){
			ridtos.add((RelationalIntegrityDTO)dto2);
		}
		return ridtos;
	}

}
