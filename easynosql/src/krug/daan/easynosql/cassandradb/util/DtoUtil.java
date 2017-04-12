package krug.daan.easynosql.cassandradb.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import krug.daan.easynosql.cassandradb.annotation.NonPersistent;
import krug.daan.easynosql.cassandradb.annotation.SecondaryIndex;
import krug.daan.easynosql.cassandradb.dto.BaseDTO;
import krug.daan.easynosql.cassandradb.exception.CassandraDataException;
import krug.daan.easynosql.common.KeyValue;

import com.datastax.driver.core.Row;

/**
 * @author Daniel Augusto Krug
 *
 * Class to handle the CQL (Cassandra Query Language) commands creation, based on a BaseDTO object
 */
public class DtoUtil {
	
	/**
	 * Simple Date Formatter
	 */
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * DANGEROUS_OPERATION_DELETE_ALL_EXCEPTION
	 */
	private static final String DANGEROUS_OPERATION_DELETE_ALL_EXCEPTION = "DELETE whithout WHERE condition: ";
	
	/**
	 * Validate if a attribute is on a provided type list for the persistence
	 */
	private static void validateDataType(Object obj) throws CassandraDataException{
		if(obj instanceof Integer 
				|| obj instanceof Long
				|| obj instanceof Double
				|| obj instanceof Float
				|| obj instanceof BigInteger
				|| obj instanceof BigDecimal
				|| obj instanceof String
				|| obj instanceof Boolean
				|| obj instanceof java.util.Date){
			throw new CassandraDataException("Unsuported data type attribute: " + obj.getClass());
		}
	}
	
	/**
	 * Return the method name based on a attribute
	 */
	private static Method getMethodByField(Method[] methods,Field field,boolean setter) throws CassandraDataException{
		validateDataType(field.getType());
		String methodName = (setter ? "set" : "get") + field.getName().substring(0,1).toUpperCase() + field.getName().substring(1);
		Method method = null;
		for(Method m: methods){
			if(m.getName().equals(methodName)){
				method = m;
				break;
			}
		}
		return method;
	}
	
	/**
	 * Format a attribute value to be used on a CQL
	 */
	private static String toStringCQL(Object obj){
		if(null==obj){
			return null;
		}
		if(obj instanceof java.util.Date){
			return ("'" + sdf.format(((java.util.Date)obj)) + "'");
		}
		if(obj instanceof String){
			return ("'" + ((String)obj) + "'");
		}
		return obj.toString();
	}
	
	/**
	 * Get a atrribute value by reflection mechanism and format on a CQL style
	 */
	@SuppressWarnings(value="all")
	private static String getStringValueCQL(BaseDTO dto, Method[] methods,Field field) 
														throws CassandraDataException, InvocationTargetException, IllegalAccessException{
		Method method = getMethodByField(methods,field,false);
		Object obj = method.invoke(dto, null);
		return toStringCQL(obj);
	}
	
	
	/**
	 * Receives a list of KeyValue objects to set values in respective attributes
	 */
	public static void setValues(BaseDTO dto,List<KeyValue> keyValues) 
			throws InvocationTargetException, IllegalAccessException, CassandraDataException{
		try {
			Method[] methods = dto.getClass().getDeclaredMethods();
			Field[] fields = dto.getClass().getDeclaredFields();
			for(Field field: fields){
				if(field.isAnnotationPresent(NonPersistent.class)){
					continue;
				}
				for(KeyValue keyValue: keyValues){
					if(keyValue.getKey().equals(field.getName())){
						Method method = getMethodByField(methods,field,true);
						if(null!=method){
							method.invoke(dto, keyValue.getValue());
						}
					}
				}
			}
		} catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Receives a com.datastax.driver.core.Row Object to set values in self respective attributes
	 */
	public static void setValues(BaseDTO dto,Row row) throws InvocationTargetException, IllegalAccessException, CassandraDataException{
		try {
			dto.setId(row.getString(BaseDTO.ID_ATTR_DESCRIPTOR));
			dto.setRelatedObjectsIdsJson(row.getString(BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON));
			Method[] methods = dto.getClass().getDeclaredMethods();
			Field[] fields = dto.getClass().getDeclaredFields();
			for(Field field: fields){
				if(field.isAnnotationPresent(NonPersistent.class)){
					continue;
				}
				Method method = getMethodByField(methods,field,true);
				Object obj = row.getObject(field.getName());
				if(null!=method){
					method.invoke(dto, obj);
				}
			}
		} catch (Exception e) {
			throw new CassandraDataException(e);
		}
	}
	
	/**
	 * Return the CassandraDB column type according the Java type of the attribute
	 */
	@SuppressWarnings(value="all")
	public static String getDataTypeForAttributeClass(Class clazz) throws CassandraDataException{
		String type = "";
		if(clazz.equals(Integer.class)){
			type = "int";
		}else if(clazz.equals(Long.class)){
			type = "bigint";
		}else if(clazz.equals(Double.class)){
			type = "double";
		}else if(clazz.equals(Float.class)){
			type = "float";
		}else if(clazz.equals(BigInteger.class)){
			type = "bigint";
		}else if(clazz.equals(BigDecimal.class)){
			type = "decimal";
		}else if(clazz.equals(String.class)){
			type = "text";
		}else if(clazz.equals(Boolean.class)){
			type = "boolean";
		}else if(clazz.equals(java.util.Date.class)){
			type = "timestamp";
		}else{
			throw new CassandraDataException("Unsuported clazz type attribute: " + clazz.getName());
		}
		return type;
	}
	
	/**
	 * Generate the table meta data, to be used on table creation
	 */
	public static List<String[]> generateTableMetadata(BaseDTO dto) throws CassandraDataException{
		List<String[]> columnNamesDatatypes = new ArrayList<String[]>();
		columnNamesDatatypes.add(new String[]{BaseDTO.ID_ATTR_DESCRIPTOR,"text","PRIMARY KEY","false"});
		columnNamesDatatypes.add(new String[]{BaseDTO.TABLE_NAME_ATTR_DESCRIPTOR,"text","","false"});
		columnNamesDatatypes.add(new String[]{BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON,"text","","false"});
		Field[] fields = dto.getClass().getDeclaredFields();
		for(Field field: fields){
			if(field.isAnnotationPresent(NonPersistent.class)){
				continue;
			}
			String type = getDataTypeForAttributeClass(field.getType());
			String createSecondaryIndex = "false";
			if(field.isAnnotationPresent(SecondaryIndex.class)){
				createSecondaryIndex = "true";
			}
			columnNamesDatatypes.add(new String[]{field.getName(),type,"",createSecondaryIndex});
		}
		return columnNamesDatatypes;
	}
	
	/**
	 * Generate the CQL INSERT command for a received BaseDTO object
	 */
	public static String generateInsertCQL(BaseDTO dto) 
			throws CassandraDataException, InvocationTargetException, IllegalAccessException{
		Field[] fields = dto.getClass().getDeclaredFields();
		Method[] methods = dto.getClass().getDeclaredMethods();
		String virgula = ",";
		StringBuffer cql = new StringBuffer();
		cql.append("INSERT INTO " + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + "(");
		cql.append(BaseDTO.ID_ATTR_DESCRIPTOR);
		cql.append(virgula + BaseDTO.TABLE_NAME_ATTR_DESCRIPTOR);
		for(Field field: fields){
			if(field.isAnnotationPresent(NonPersistent.class)){
				continue;
			}
			cql.append(virgula + field.getName());
		}
		cql.append(virgula + BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON);
		cql.append(") values (");
		cql.append("'" + dto.getId() + "'");	
		cql.append(virgula + "'" + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + "'");	
		for(Field field: fields){
			if(field.isAnnotationPresent(NonPersistent.class)){
				continue;
			}
			String value = getStringValueCQL(dto,  methods, field) ;
			cql.append(virgula + value);
		}
		cql.append(virgula + "'" + dto.getRelatedObjectsIdsJson() + "'");
		cql.append(");");
		return cql.toString();
	}
	
	/**
	 * Generate the "where condition" part of a CQL command
	 */
	private static String generateWhereConditions(Field[] fields,List<KeyValue> updateConditions) throws CassandraDataException{
		String andWhereCondition = null;
		StringBuffer whereConditions = new StringBuffer();
		for(Field field: fields){
			if(field.isAnnotationPresent(NonPersistent.class)){
				continue;
			}
			validateDataType(field.getType());
			for(KeyValue keyValue: updateConditions){
				if(keyValue.getKey().equals(field.getName())){
					if(null==keyValue.getValue()){
						break;
					}
					andWhereCondition = (whereConditions.length() > 0) ? " AND " : " WHERE ";
					String value = toStringCQL(keyValue.getValue());
					whereConditions.append(andWhereCondition + field.getName() + " = " + value);
					break;
				}
			}
		}
		return whereConditions.toString();
	}
	
	/**
	 * Generate the CQL UPDATE command for a received BaseDTO object that will applied to a list of 
	 * BaseDTO objects of same class
	 */
	public static String generateUpdateCQL(BaseDTO dto,List<KeyValue> updateParameters, Collection<BaseDTO> dtos) 
									throws CassandraDataException, InvocationTargetException, IllegalAccessException{
		StringBuffer cql = new StringBuffer();
		cql.append("UPDATE " + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + " SET ");
		String virgula = "";
		if(null!=dto.getId() && dto.getId().trim().length() > 0 
				&& null==updateParameters && (null==dtos || dtos.size() == 0)){
			updateParameters = generateKeyValuesAttributes(dto);
		}
		for(KeyValue kv : updateParameters){
			cql.append(virgula + kv.getKey() + " = " + toStringCQL(kv.getValue()));
			virgula = ",";
		}
		if(null!=dto.getId() && dto.getId().trim().length() > 0){
			cql.append(" WHERE id = '" + dto.getId() + "'");
		}else if(null!=dtos && dtos.size() > 0){
			cql.append(" WHERE id IN ( ");
			virgula = "";
			for(BaseDTO dto2: dtos){
				cql.append(virgula + "'" + dto2.getId() + "'");
				virgula = ",";
			}
			cql.append(") ");
		}
		cql.append(";"); 
		return cql.toString();
	}
	
	/**
	 * Create the mapping from object attributes to a list of KeyValue objects
	 */
	private static List<KeyValue> generateKeyValuesAttributes(BaseDTO dto) throws InvocationTargetException, IllegalAccessException{
		List<KeyValue> attrsValues = new ArrayList<KeyValue>();
		Method[] BaseDTOMethods = BaseDTO.class.getDeclaredMethods();
		Field[] BaseDTOFields = BaseDTO.class.getDeclaredFields();
		for(Field field: BaseDTOFields){
			if(field.isAnnotationPresent(NonPersistent.class)){
				continue;
			}
			if(field.getName().equals(BaseDTO.TABLE_NAME_ATTR_DESCRIPTOR)
					|| field.getName().equals(BaseDTO.ID_ATTR_DESCRIPTOR)
					|| field.getName().equals(BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS)
					|| field.getName().equals(BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS_IDS)
					){
				continue;
			}
			if(field.getName().equals(BaseDTO.KEYVALUES_ATTR_RELATED_OBJECTS_IDS_JSON)){
				if(null!=dto.getRelatedObjectsIds()){
					StringBuffer json = new StringBuffer();
					String separator = "";
					for(KeyValue keyValue : dto.getRelatedObjectsIds()){
						json.append(separator + keyValue.getKey() + "," + keyValue.getValue());
						separator = ";";
					}
					dto.setRelatedObjectsIdsJson(json.toString());
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
				Object obj = method.invoke(dto, null);
				attrsValues.add(new KeyValue(field.getName(),obj));
			}
		}
		Method[] methods = dto.getClass().getDeclaredMethods();
		Field[] fields = dto.getClass().getDeclaredFields();
		for(Field field: fields){
			if(field.isAnnotationPresent(NonPersistent.class)){
				continue;
			}
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
				Object obj = method.invoke(dto, null);
				attrsValues.add(new KeyValue(field.getName(),obj));
			}
		}
		return attrsValues;
	}
	
	/**
	 * Verify if the CQL command need use the "ALLOW FILTERING" clause
	 * when exists more that one column on the "where" clause
	 */
	private static boolean needAllowFiltering(BaseDTO dto,List<KeyValue> searchConditions){
		if(null==searchConditions || searchConditions.size() > 2){
			return false;
		}
		int countIdx = 0;
		for(Field field : dto.getClass().getDeclaredFields()){
			if(!field.isAnnotationPresent(SecondaryIndex.class)){
				continue;
			}
			for(KeyValue kv : searchConditions){
				if(kv.getKey().equals(field.getName())){
					countIdx ++;
					break;
				}
			}
		}
		return (countIdx > 1);
	}
	
	/**
	 * Create a CQL SELECT command according the received BaseDTO object
	 */
	public static String generateSelectCQL(BaseDTO dto,List<KeyValue> searchConditions) 
												throws CassandraDataException, InvocationTargetException, IllegalAccessException{
		StringBuffer cql = new StringBuffer();
		cql.append("SELECT * FROM " + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + " ");
		String whereConditionCQL = "";
		if(null!=searchConditions && searchConditions.size() > 0){
			whereConditionCQL += generateWhereConditions(dto.getClass().getDeclaredFields(),searchConditions);
		}
		if(whereConditionCQL.trim().length() > 0){
			cql.append(whereConditionCQL); 
		}else if(null!=dto.getId() && dto.getId().trim().length() > 0){
			cql.append(" WHERE id = '" + dto.getId() + "'"); 
		}
		
		if(needAllowFiltering(dto,searchConditions)){
			cql.append(" ALLOW FILTERING ");
		}
		cql.append(";");
		return cql.toString();
	}
	
	/**
	 * Generate the CQL DELETE command according a received BaseDTO object
	 */
	public static String generateDeleteCQL(BaseDTO dto,List<KeyValue> searchConditions) 
			throws CassandraDataException, InvocationTargetException, IllegalAccessException{
		StringBuffer cql = new StringBuffer();
		cql.append("DELETE FROM " + dto.getTableName().substring(dto.getTableName().lastIndexOf(".") + 1) + " ");
		String whereConditionCQL = "";
		if(null!=searchConditions && searchConditions.size() > 0){
			whereConditionCQL += generateWhereConditions(dto.getClass().getDeclaredFields(),searchConditions);
		}
		if(whereConditionCQL.trim().length() > 0){
			cql.append(whereConditionCQL); 
		}else if(null!=dto.getId() && dto.getId().trim().length() > 0){
			cql.append(" WHERE id = '" + dto.getId() + "'"); 
		}else{
			throw new CassandraDataException(DANGEROUS_OPERATION_DELETE_ALL_EXCEPTION + cql.toString());
		}
		cql.append(";");
		return cql.toString();
	}
	
	

}
