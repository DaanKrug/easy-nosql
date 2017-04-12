package krug.daan.easynosql.cassandradb.dao;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import krug.daan.easynosql.cassandradb.config.CassandraConfig;
import krug.daan.easynosql.cassandradb.dto.BaseDTO;
import krug.daan.easynosql.cassandradb.dto.RelationalIntegrityDTO;
import krug.daan.easynosql.cassandradb.exception.CassandraDataException;
import krug.daan.easynosql.cassandradb.util.DtoUtil;
import krug.daan.easynosql.common.KeyValue;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * @author Daniel Augusto Krug
 *
 * Class to handle the basic CRUD operations on the CassandraDB mechanism, 
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
	private static final String OBJECT_ID_PARAMETER_EMPTY_EXCEPTION = "<BaseDTO.id> attribute couldnt be null or empty.";
	
	/**
	 * DUPLICATED_OBJECT_ID_EXCEPTION
	 */
	private static final String DUPLICATED_OBJECT_ID_EXCEPTION = "Duplicated object id: ";
	
	/**
	 * GHOST_OBJECT_ID_EXCEPTION
	 */
	private static final String GHOST_OBJECT_ID_EXCEPTION = "The Object dont exist or was deleted. Searched object id: ";
	
	/**
	 * RELATED_OBJECTS_INTEGRITY_EXCEPTION
	 */
	private static final String RELATED_OBJECTS_INTEGRITY_EXCEPTION = "These Objects can not be deleted due to existing Relational Integrity: ";
	
	/**
	 * Cassandra DB configuration Object
	 */
	private CassandraConfig csc;
	
	/**
	 * Already created tables
	 */
	private List<String> existingTables;
	
	/**
	 * Singleton instance
	 */
	private static BaseDAO instance;
	
	/**
	 * Return the singleton instance
	 */
	public static BaseDAO getInstance(CassandraConfig csc){
		if(null==instance){
			instance = new BaseDAO(csc);
		}
		return instance;
	}
	
	/**
	 * Constructor
	 */
	protected BaseDAO(CassandraConfig csc){
		this.csc = csc;
		initializeDatabase();
		existingTables = new ArrayList<String>();
	}
	
	/**
	 * Initialize the current CassandraDB database configuration
	 */
	private void initializeDatabase(){
		try {
			CassandraDAO.getInstance(csc).createOrUpdateDatabase(csc.getDatabaseName(),csc.isNetworkStrategy(),csc.getReplicationFactor(),csc.isDurableWrites(),true,true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			CassandraDAO.getInstance(csc).useDatabase(csc.getDatabaseName());
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			CassandraDAO.getInstance(csc).createTable(new RelationalIntegrityDTO(), true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Creates a new Object.
	 */
	public void saveOrUpdate(BaseDTO dto) throws CassandraDataException{
		if(null==dto.getTableName() || dto.getTableName().trim().length() == 0){
			throw new CassandraDataException(TABLE_NAME_EMPTY_EXCEPTION);
		}
		try {
			if(!existingTables.contains(dto.getTableName())){
				if(CassandraDAO.getInstance(csc).createTable(dto, true)){
					existingTables.add(dto.getTableName());
				}
			}
			if(!(dto instanceof RelationalIntegrityDTO)){
				dto.populateRelatedObjectsIdsJson();
			}
			if(null!=dto.getId() && dto.getId().trim().length() > 0){
				update(dto);
				return;
			}
			dto.generateId();
			String query = DtoUtil.generateInsertCQL(dto);
			csc.logInfo(query);
			csc.getSession().execute(query);
		} 
		catch (CassandraDataException cde){
			throw (cde);
		}
		catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Update values of attributes of a one specific Object [dto].
	 * The values will be updated according the actual values of a object.
	 */
	public void update(BaseDTO dto) throws CassandraDataException{
		if(null==dto.getId() || dto.getId().trim().length() == 0){
			throw new CassandraDataException(OBJECT_ID_PARAMETER_EMPTY_EXCEPTION);
		}
		try {
			String query = DtoUtil.generateUpdateCQL(dto,null,null);
			csc.logInfo(query);
			csc.getSession().execute(query);
			if(!(dto instanceof RelationalIntegrityDTO)){
				RelationalIntegrityDAO.getInstance(csc).updateRelationalIntegrityDTO(dto);
			}
		} 
		catch (CassandraDataException cde){
			throw (cde);
		}
		catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Update values of attributes of a one specific object [dto] if updateConditions is null.
	 * Otherwise update all objects that matches with [updateConditions].
	 * Return the number of updated objects.
	 */
	public void update(BaseDTO dto, List<KeyValue> updateParameters) throws CassandraDataException{
		if(null==dto.getId() || dto.getId().trim().length() == 0){
			throw new CassandraDataException(OBJECT_ID_PARAMETER_EMPTY_EXCEPTION);
		}
		if(null==dto.getTableName() || dto.getTableName().trim().length() == 0){
			throw new CassandraDataException(TABLE_NAME_EMPTY_EXCEPTION);
		}
		if(null==updateParameters || updateParameters.size() == 0){
			throw new CassandraDataException(UPDATE_PARAMETERS_EMPTY_EXCEPTION);
		}
		try {
			String query = DtoUtil.generateUpdateCQL(dto,updateParameters,null);
			csc.logInfo(query);
			csc.getSession().execute(query);
			if(!(dto instanceof RelationalIntegrityDTO)){
				RelationalIntegrityDAO.getInstance(csc).updateRelationalIntegrityDTO(dto);
			}
		} 
		catch (CassandraDataException cde){
			throw (cde);
		}
		catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Update all objects that matches [searchParameters] whit the [updateParameters] values.
	 * Return the number of updated objects.
	 */
	public Integer updateAll(BaseDTO dto,List<KeyValue> searchParameters, List<KeyValue> updateParameters) 
			throws CassandraDataException{
		if(null==dto.getTableName() || dto.getTableName().trim().length() == 0){
			throw new CassandraDataException(TABLE_NAME_EMPTY_EXCEPTION);
		}
		if(null==searchParameters || searchParameters.size() == 0){
			throw new CassandraDataException(SEARCH_PARAMETERS_EMPTY_EXCEPTION);
		}
		if(null==updateParameters || updateParameters.size() == 0){
			throw new CassandraDataException(UPDATE_PARAMETERS_EMPTY_EXCEPTION);
		}
		try {
			Collection<BaseDTO> dtos = find(dto,searchParameters);
			if(null==dtos || dtos.size() == 0){
				return 0;
			}
			DtoUtil.setValues(dtos.iterator().next(),searchParameters);
			String query = DtoUtil.generateUpdateCQL(dtos.iterator().next(),updateParameters,dtos);
			csc.logInfo(query);
			csc.getSession().execute(query);
			return dtos.size();
		} 
		catch (CassandraDataException cde){
			throw (cde);
		}
		catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Return all objects that matches whit the [searchParameters] values.
	 * if [searchParameters>]is empty, then the [searchParameters] will be obtained by the 
	 * BaseDTO.getAllAttributeValues() Method.
	 */
	public Collection<BaseDTO> find(BaseDTO dto,List<KeyValue> searchParameters) 
			throws CassandraDataException, InvocationTargetException, IllegalAccessException, InstantiationException{
		return find(dto,searchParameters,false);
	}

	/**
	 * Return all objects that matches whit the [searchParameters] values.
	 * if [searchParameters>]is empty, then the [searchParameters] will be obtained by the 
	 * BaseDTO.getAllAttributeValues() Method.
	 */
	private Collection<BaseDTO> find(BaseDTO dto,List<KeyValue> searchParameters, boolean forRelationsValidation) 
			throws CassandraDataException, InvocationTargetException, IllegalAccessException, InstantiationException{
		if(null==dto.getTableName() || dto.getTableName().trim().length() == 0){
			throw new CassandraDataException(TABLE_NAME_EMPTY_EXCEPTION);
		}
		try {
			Collection<BaseDTO> dtos = new ArrayList<BaseDTO>();
			String query = DtoUtil.generateSelectCQL(dto,searchParameters);
			csc.logInfo(query);
			ResultSet rs = csc.getSession().execute(query);
			List<Row> rows = rs.all();
			for(Row row : rows){
				BaseDTO dto2 = dto.getClass().newInstance();
				DtoUtil.setValues(dto2,row);
				if(!forRelationsValidation){
					dto2.populateRelatedObjectsIds();
					// initialize related objects
					dto2.setRelatedObjects(new ArrayList<KeyValue>());
					for(KeyValue keyValue: dto2.getRelatedObjectsIds()){
						try {
							@SuppressWarnings(value="all")
							Class clazz = Class.forName(keyValue.getKey());
							BaseDTO relatedDTO = (BaseDTO)clazz.newInstance();
							relatedDTO.setId((String)keyValue.getValue());
							BaseDTO dtoToAdd = findById(relatedDTO, false);
							dto2.getRelatedObjects().add(new KeyValue(keyValue.getKey(), dtoToAdd));
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				dtos.add(dto2);
			}
			return dtos;
		} 
		catch (CassandraDataException cde){
			throw (cde);
		}
		catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Return a object from a table that match the specific "id"
	 */
	public BaseDTO findById(BaseDTO dto,boolean initializeRelatedObjects) 
			throws CassandraDataException, InvocationTargetException, IllegalAccessException, InstantiationException{
		if(null==dto.getId() || dto.getId().trim().length() == 0){
			throw new CassandraDataException(OBJECT_ID_PARAMETER_EMPTY_EXCEPTION);
		}
		Collection<BaseDTO> dtos = new ArrayList<BaseDTO>();
		try {
			String query = DtoUtil.generateSelectCQL(dto,null);
			csc.logInfo(query);
			ResultSet rs = csc.getSession().execute(query);
			List<Row> rows = rs.all();
			for(Row row : rows){
				BaseDTO dto2 = dto.getClass().newInstance();
				DtoUtil.setValues(dto2,row);
				dto2.populateRelatedObjectsIds();
				if(initializeRelatedObjects){
					dto2.setRelatedObjects(new ArrayList<KeyValue>());
					for(KeyValue keyValue: dto2.getRelatedObjectsIds()){
						try {
							@SuppressWarnings(value="all")
							Class clazz = Class.forName(keyValue.getKey());
							BaseDTO relatedDTO = (BaseDTO)clazz.newInstance();
							relatedDTO.setId((String)keyValue.getValue());
							BaseDTO dtoToAdd = findById(relatedDTO, false);
							dto2.getRelatedObjects().add(new KeyValue(keyValue.getKey(), dtoToAdd));
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				dtos.add(dto2);
			}
		} 
		catch (CassandraDataException cde){
			throw (cde);
		}
		catch (Exception e) {
			throw new CassandraDataException(e);
		}
		if(dtos.size() == 0){
			throw new CassandraDataException(GHOST_OBJECT_ID_EXCEPTION + " " + dto.getId());
		}else if(dtos.size() > 1){
			throw new CassandraDataException(DUPLICATED_OBJECT_ID_EXCEPTION + " " + dto.getId());
		}
		return dtos.iterator().next();
	}
	
	/**
	 * Validate the RelationalIntegrity objects from a BaseDTO that want be excluded,
	 * if exists one or more RelationalIntegrity whit this object in other(s) object(s)
	 * a CassandraDataException is throw.
	 */
	private void validateExclusion(BaseDTO dto) 
			throws InstantiationException, IllegalAccessException, InvocationTargetException, CassandraDataException{
		if(!(dto instanceof RelationalIntegrityDTO)){
			StringBuffer exclusionRestrictionsMsgs = new StringBuffer();
			Collection<RelationalIntegrityDTO> ridtos = RelationalIntegrityDAO.getInstance(csc).getLockedByRelationalIntegrity(dto);
			if(null!=ridtos && ridtos.size() > 0){
				exclusionRestrictionsMsgs.append("\nObject class: " + dto.getClass().getName() + " Object _id: " +  dto.getId());
				exclusionRestrictionsMsgs.append(" whit relational restrictions on:");
				for(RelationalIntegrityDTO ridto: ridtos){
					exclusionRestrictionsMsgs.append("\n\t class: " + ridto.getOwnerTableName() + " _id: " + ridto.getOwnerId());
				}
			}
			if(exclusionRestrictionsMsgs.length() > 0){
				throw new CassandraDataException(RELATED_OBJECTS_INTEGRITY_EXCEPTION + exclusionRestrictionsMsgs.toString());
			}
		}
	}
	
	/**
	 * Delete a object by her "id" value
	 */
	public void deleteById(BaseDTO dto) throws CassandraDataException{
		if(null==dto.getId() || dto.getId().trim().length() == 0){
			throw new CassandraDataException(OBJECT_ID_PARAMETER_EMPTY_EXCEPTION);
		}
		try {
			validateExclusion(dto);
			String query = DtoUtil.generateDeleteCQL(dto,null);
			csc.logInfo(query);
			csc.getSession().execute(query);
		} 
		catch (CassandraDataException cde){
			throw (cde);
		}
		catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Delete all objects that matches whit the [searchParameters] values.
	 * if [searchParameters] is empty, then the [searchParameters] will be obtained by the 
	 * BaseDTO.getAllAttributeValues() Method.
	 */
	public Integer deleteAll(BaseDTO dto,List<KeyValue> searchParameters) throws CassandraDataException{
		if(null==dto.getTableName() || dto.getTableName().trim().length() == 0){
			throw new CassandraDataException(TABLE_NAME_EMPTY_EXCEPTION);
		}
		if( (null==searchParameters || searchParameters.size() == 0) ){
			throw new CassandraDataException(DELETE_ATTRIBUTES_EMPTY_EXCEPTION);
		}
		try {
			Collection<BaseDTO> dtos = find(dto,searchParameters,true);
			for(BaseDTO dto2 : dtos){
				validateExclusion(dto2);
			}
			int count = 0;
			for(BaseDTO dto2 : dtos){
				deleteById(dto2);
				if(!(dto2 instanceof RelationalIntegrityDTO)){
					List<KeyValue> relationalIntegritiesToRemove = new ArrayList<KeyValue>();
					relationalIntegritiesToRemove.add(new KeyValue(dto2.getTableName(),dto2.getId()));
					RelationalIntegrityDAO.getInstance(csc).removeRelationalIntegrityDTO(relationalIntegritiesToRemove);
				}
				count ++;
			}
			return count;
		} 
		catch (CassandraDataException cde){
			throw (cde);
		}
		catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}

}
