package krug.daan.easynosql.cassandradb.dao;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import krug.daan.easynosql.cassandradb.config.CassandraConfig;
import krug.daan.easynosql.cassandradb.dto.BaseDTO;
import krug.daan.easynosql.cassandradb.dto.RelationalIntegrityDTO;
import krug.daan.easynosql.cassandradb.exception.CassandraDataException;
import krug.daan.easynosql.common.KeyValue;

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
	public static RelationalIntegrityDAO getInstance(CassandraConfig csc){
		if(null==instance){
			instance = new RelationalIntegrityDAO(csc);
		}
		return instance;
	}
	
	/**
	 * Constructor
	 */
	private RelationalIntegrityDAO(CassandraConfig csc){
		super(csc);
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
			throws InstantiationException, IllegalAccessException,InvocationTargetException, CassandraDataException{
		List<KeyValue> searchParameters = new ArrayList<KeyValue>();
		searchParameters.add(new KeyValue(RelationalIntegrityDTO.OWNER_ID,dto.getId()));
		searchParameters.add(new KeyValue(RelationalIntegrityDTO.OWNER_TABLE_NAME,dto.getTableName()));
		deleteAll(new RelationalIntegrityDTO(), searchParameters);
		if(null!=dto.getRelatedObjectsIds() && dto.getRelatedObjectsIds().size() > 0){
			List<RelationalIntegrityDTO> newRelations = new ArrayList<RelationalIntegrityDTO>();
			for(KeyValue keyValue: dto.getRelatedObjectsIds()){
				RelationalIntegrityDTO ridto = new RelationalIntegrityDTO();
				ridto.setOwnerId(dto.getId());
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
	 * Remove the [relationalIntegrityDTO] objects that matches whit [relationalIntegrities]
	 */
	public void removeRelationalIntegrityDTO(List<KeyValue> relationalIntegrities) throws CassandraDataException{
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
			throws InstantiationException, IllegalAccessException, InvocationTargetException, CassandraDataException{
		List<KeyValue> searchParameters = new ArrayList<KeyValue>();
		searchParameters.add(new KeyValue(RelationalIntegrityDTO.RELATED_ID,dto.getId()));
		searchParameters.add(new KeyValue(RelationalIntegrityDTO.RELATED_TABLE_NAME,dto.getTableName()));
		Collection<BaseDTO> dtos =  find(new RelationalIntegrityDTO(), searchParameters);
		Collection<RelationalIntegrityDTO> ridtos = new ArrayList<RelationalIntegrityDTO>();
		for(BaseDTO dto2 : dtos){
			ridtos.add((RelationalIntegrityDTO)dto2);
		}
		return ridtos;
	}

}
