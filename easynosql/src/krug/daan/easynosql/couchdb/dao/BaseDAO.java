package krug.daan.easynosql.couchdb.dao;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import krug.daan.easynosql.common.KeyValue;
import krug.daan.easynosql.couchdb.config.CouchConfig;
import krug.daan.easynosql.couchdb.dto.BaseDTO;
import krug.daan.easynosql.couchdb.dto.RelationalIntegrityDTO;
import krug.daan.easynosql.couchdb.exception.CouchDataException;

import org.lightcouch.Response;


/**
 * @author Daniel Augusto Krug
 *
 * Class to handle the basic CRUD operations on the CouchDB mechanism, 
 * making transparent the data access details to programmers.
 */
public class BaseDAO {
	
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
	private static final String UPDATE_PARAMETERS_EMPTY_EXCEPTION = "<updateParameters> parameter couldnt be null or empty.";
	
	/**
	 * OBJECT_ID_PARAMETER_EMPTY_EXCEPTION
	 */
	private static final String OBJECT_ID_PARAMETER_EMPTY_EXCEPTION = "<oid> parameter couldnt be null or empty.";
	
	/**
	 * OBJECT_ID_PARAMETER_EMPTY_EXCEPTION
	 */
	private static final String OBJECT_ID_PARAMETER_INVALID_EXCEPTION = "<oid> parameter should be a <String> or <ObjectId> type.";
	
	/**
	 * GHOST_OBJECT_ID_EXCEPTION
	 */
	private static final String GHOST_OBJECT_ID_EXCEPTION = "The Object dont exist or was deleted. Searched object id: ";
	
	/**
	 * INCORRECT_OBJECT_CLASS_INFO_EXCEPTION
	 */
	private static final String INCORRECT_OBJECT_CLASS_INFO_EXCEPTION = "The Object exist but whit other class type information. Searched object id: ";
	
	/**
	 * RELATED_OBJECTS_INTEGRITY_EXCEPTION
	 */
	private static final String RELATED_OBJECTS_INTEGRITY_EXCEPTION = "These Objects can not be deleted due to existing Relational Integrity: ";
	
	/**
	 * DELETE_OBJECTS_EXCEPTION
	 */
	private static final String DELETE_OBJECTS_EXCEPTION = "These Objects can not be deleted due to errors: ";
	
	
	/**
	 * Couch DB configuration Object
	 */
	private CouchConfig chc;
	
	/**
	 * Singleton instance
	 */
	private static BaseDAO instance;
	
	/**
	 * Return the singleton instance
	 */
	public static BaseDAO getInstance(CouchConfig chc){
		if(null==instance){
			instance = new BaseDAO(chc);
		}
		return instance;
	}
	
	/**
	 * Constructor
	 */
	protected BaseDAO(CouchConfig chc){
		this.chc = chc;
	}
	
	/**
	 * Creates a new Object.
	 */
	public Response saveOrUpdate(BaseDTO dto) throws CouchDataException{
		try {
			Response res = null;
			if(null!=dto.getId() && dto.getId().trim().length() > 0){
				res = chc.getDbClient().update(dto);
				if(!(dto instanceof RelationalIntegrityDTO)){
					RelationalIntegrityDAO.getInstance(chc).updateRelationalIntegrityDTO(dto);
				}
			}else{
				dto.setTableNameControl(dto.getTableName());
				res = chc.getDbClient().save(dto);
			}
			return res;
		} 
		catch (CouchDataException ce){
			throw (ce);
		}
		catch (Exception e) {
			throw new CouchDataException(e);
		}
	}
	
	/**
	 * Update all objects that matches [searchParameters] whit the [updateParameters] values.
	 */
	public List<Response> updateAll(BaseDTO dto,List<KeyValue> searchParameters, List<KeyValue> updateParameters) 
			throws CouchDataException{
		
		if(null==searchParameters || searchParameters.size() == 0){
			throw new CouchDataException(SEARCH_PARAMETERS_EMPTY_EXCEPTION);
		}
		if(null==updateParameters || updateParameters.size() == 0){
			throw new CouchDataException(UPDATE_PARAMETERS_EMPTY_EXCEPTION);
		}
		try {
			List<BaseDTO> allDtos = find(dto,searchParameters);
			List<Response> resps = new ArrayList<Response>();
			for(BaseDTO dto2: allDtos){
				dto2.setValues(updateParameters);
				resps.add(saveOrUpdate(dto2));
			}
			return resps;
		} 
		catch (CouchDataException ce) {
			throw ce;
		}
		catch (Exception e) {
			throw new CouchDataException(e);
		}
	}
	
	/**
	 * Initialize attribute values
	 */
	private BaseDTO initialize(BaseDTO dto){
		@SuppressWarnings(value="all")
		Class clazz = dto.getClass();
		@SuppressWarnings(value="all")
		BaseDTO dto2 = chc.getDbClient().find(clazz,dto.getId());
		dto2.setId(dto.getId());
		return dto2;
	}
	
	/**
	 * Find a list of all objects of a class and that match whit [searchParameters]
	 */
	public List<BaseDTO> find(BaseDTO dto, List<KeyValue> searchParameters) throws CouchDataException {
		try {
			@SuppressWarnings(value="all")
			Class clazz = dto.getClass();
			@SuppressWarnings(value="all")
			List<BaseDTO> allDtos = chc.getDbClient().view("_all_docs").query(clazz);
			List<BaseDTO> dtosMatched = new ArrayList<BaseDTO>();
			if(null!=allDtos){
				for(BaseDTO dto2: allDtos){
					dto2 = initialize(dto2);
					if(!(null!=dto2.getTableNameControl() && dto2.getTableNameControl().trim().equals(dto.getTableName().trim()))){
						continue;
					}
					if(null==searchParameters || dto2.matchKeyValues(searchParameters)){
						dtosMatched.add(dto2);
					}
				}
			}
			return dtosMatched;
		} catch (Exception e) {
			throw new CouchDataException(e);
		}
	}
	
	/**
	 * Return a object from a table that match the specific "_id"
	 */
	public BaseDTO findById(BaseDTO dto,String oid, boolean initializeRelatedObjects) 
			throws CouchDataException, InvocationTargetException, IllegalAccessException, InstantiationException{
		if(null==oid){
			throw new CouchDataException(OBJECT_ID_PARAMETER_EMPTY_EXCEPTION);
		}
		if(oid.trim().length() != 24){
			throw new CouchDataException(OBJECT_ID_PARAMETER_INVALID_EXCEPTION);
		}
		@SuppressWarnings(value="all")
		Class clazz = dto.getClass();
		@SuppressWarnings(value="all")
		BaseDTO dto2 = chc.getDbClient().find(clazz, oid);
		if(null==dto2){
			throw new CouchDataException(GHOST_OBJECT_ID_EXCEPTION + " " + oid);
		}
		if(!(null!=dto2.getTableNameControl() && dto2.getTableNameControl().trim().equals(dto.getTableName().trim()))){
			throw new CouchDataException(INCORRECT_OBJECT_CLASS_INFO_EXCEPTION + " " + oid);
		}
		dto2.setId(oid);
		dto2.populateRelatedObjectsIds();
		if(initializeRelatedObjects){
			dto2.setRelatedObjects(new ArrayList<KeyValue>());
			for(KeyValue keyValue: dto.getRelatedObjectsIds()){
				try {
					@SuppressWarnings(value="all")
					Class clazz2 = Class.forName(keyValue.getKey());
					BaseDTO relatedDTO = (BaseDTO)clazz2.newInstance();
					BaseDTO dtoToAdd = findById(relatedDTO, (String)keyValue.getValue(), false);
					dto2.getRelatedObjects().add(new KeyValue(keyValue.getKey(), dtoToAdd));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return dto2;
	}
	
	/**
	 * Delete a object by her "_id" value
	 */
	public Response deleteById(BaseDTO dto) throws CouchDataException{
		try {
			BaseDTO dtoToDelete = findById(dto, dto.getId(), false);
			if(!(dtoToDelete instanceof RelationalIntegrityDTO)){
				StringBuffer exclusionRestrictionsMsgs = new StringBuffer();
				Collection<RelationalIntegrityDTO> ridtos = RelationalIntegrityDAO.getInstance(chc).getLockedByRelationalIntegrity(dtoToDelete);
				if(null!=ridtos && ridtos.size() > 0){
					exclusionRestrictionsMsgs.append("\nObject class: " + dtoToDelete.getClass().getName() + " Object _id: " +  dtoToDelete.getId());
					exclusionRestrictionsMsgs.append(" whit relational restrictions on:");
					for(RelationalIntegrityDTO ridto: ridtos){
						exclusionRestrictionsMsgs.append("\n\t class: " + ridto.getOwnerTableName() + " _id: " + ridto.getOwnerId());
					}
				}
				if(exclusionRestrictionsMsgs.length() > 0){
					throw new CouchDataException(RELATED_OBJECTS_INTEGRITY_EXCEPTION + exclusionRestrictionsMsgs.toString());
				}
			}
			Response resp = chc.getDbClient().remove(dtoToDelete);
			if(null==resp.getError() || resp.getError().trim().length() == 0){
				List<KeyValue> relationalIntegritiesToRemove = new ArrayList<KeyValue>();
				relationalIntegritiesToRemove.add(new KeyValue(dtoToDelete.getClass().getName(),dtoToDelete.getId()));
				RelationalIntegrityDAO.getInstance(chc).removeRelationalIntegrityDTO(relationalIntegritiesToRemove);
			}else{
				String exclusionErrorMsgs = DELETE_OBJECTS_EXCEPTION + "\n\tObject class: ";
				exclusionErrorMsgs += dtoToDelete.getClass().getName() + " Object _id: " +  dtoToDelete.getId();
				exclusionErrorMsgs += " could not be deleted. Message: " + resp.getError();
				throw new CouchDataException(exclusionErrorMsgs);
			}
			return resp;
		} 
		catch (CouchDataException me){
			throw (me);
		}
		catch (Exception e) {
			throw new CouchDataException(e);
		}
	}
	
	/**
	 * Delete all objects that matches whit the [searchParameters] values.
	 */
	public List<Response> deleteAll(BaseDTO dto,List<KeyValue> searchParameters) throws CouchDataException{
		
		if((null==searchParameters || searchParameters.size() == 0) 
				&& (null==dto.getAllAttributeValues() || dto.getAllAttributeValues().size() == 0)){
			throw new CouchDataException(DELETE_ATTRIBUTES_EMPTY_EXCEPTION);
		}
		try {
			List<Response> resps = new ArrayList<Response>();
			if(null==searchParameters){
				searchParameters = new ArrayList<KeyValue>();
				dto.generateKeyValues();
				for(KeyValue keyValue: dto.getAllAttributeValues()){
					if(null!=keyValue.getValue()){
						searchParameters.add(keyValue);
					}
				}
			}
			List<BaseDTO> dtosToDelete = find(dto, searchParameters);
			StringBuffer exclusionRestrictionsMsgs = new StringBuffer();
			if(!(dto instanceof RelationalIntegrityDTO)){
				for(BaseDTO dtoToDelete: dtosToDelete){
					Collection<RelationalIntegrityDTO> ridtos = RelationalIntegrityDAO.getInstance(chc).getLockedByRelationalIntegrity(dtoToDelete);
					if(null!=ridtos && ridtos.size() > 0){
						exclusionRestrictionsMsgs.append("\nObject class: " + dtoToDelete.getClass().getName() + " Object _id: " +  dtoToDelete.getId());
						exclusionRestrictionsMsgs.append(" whit relational restrictions on:");
						for(RelationalIntegrityDTO ridto: ridtos){
							exclusionRestrictionsMsgs.append("\n\t class: " + ridto.getOwnerTableName() + " _id: " + ridto.getOwnerId());
						}
					}
				}
				if(exclusionRestrictionsMsgs.length() > 0){
					throw new CouchDataException(RELATED_OBJECTS_INTEGRITY_EXCEPTION + exclusionRestrictionsMsgs.toString());
				}
			}
			StringBuffer exclusionErrorMsgs = new StringBuffer();
			List<KeyValue> relationalIntegritiesToRemove = new ArrayList<KeyValue>();
			for(BaseDTO dtoToDelete: dtosToDelete){
				Response resp = chc.getDbClient().remove(dtoToDelete);
				if(null==resp.getError() || resp.getError().trim().length() == 0){
					relationalIntegritiesToRemove.add(new KeyValue(dtoToDelete.getClass().getName(),dtoToDelete.getId()));
				}else{
					exclusionErrorMsgs.append("\n\tObject class: " + dtoToDelete.getClass().getName() + " Object _id: " +  dtoToDelete.getId());
					exclusionErrorMsgs.append(" could not be deleted. Message: " + resp.getError());
				}
				resps.add(resp);
			}
			if(!(dto instanceof RelationalIntegrityDTO)){
				RelationalIntegrityDAO.getInstance(chc).removeRelationalIntegrityDTO(relationalIntegritiesToRemove);
				if(exclusionErrorMsgs.length() > 0){
					throw new CouchDataException(DELETE_OBJECTS_EXCEPTION + exclusionErrorMsgs.toString());
				}
			}
			return resps;
		} 
		catch (CouchDataException ce){
			throw (ce);
		}
		catch (Exception e) {
			throw new CouchDataException(e);
		}
	}

}
