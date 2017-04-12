package krug.daan.easynosql.dynamodb.aws;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import krug.daan.easynosql.common.KeyValue;
import krug.daan.easynosql.dynamodb.exception.DynamoDataException;
import krug.daan.easynosql.dynamodb.type.ComparisonType;
import krug.daan.easynosql.dynamodb.type.DataType;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * @author Daniel Augusto Krug
 * 
 * Class to simplify the mapping values of a key-value pairs, on handling
 * of diverse database operations.
 * Add specific attributes to handle whit DynamoDB.
 */
public class AwsKeyValue extends KeyValue{

	/**
	 * Indicates the data type type for query(ies)
	 */
	private DataType type;
	
	/**
	 * Indicates the comparison type for query(ies)
	 */
	private ComparisonType ctype;
	
	/**
	 * Constructor
	 */
	public AwsKeyValue(String key, Object value, DataType type,ComparisonType ctype) {
		super(key,value);
		this.type = type;
		this.ctype = ctype;
	}

	/**
	 * Auxiliary function to create a AttributeValue object for query(ies)
	 */
	public AttributeValue generateAtributeValue() throws DynamoDataException{
		try {
			AttributeValue av = new AttributeValue();
			if(null==value){
				av.setNULL(true);
			}else if(type.equals(DataType.STRING)){
				av.setS((String)value);
			} else if(type.equals(DataType.INTEGER)){
				av.setN(((Integer)value).toString());
			} else if(type.equals(DataType.LONG)){
				av.setN(((Long)value).toString());
			} else if(type.equals(DataType.DOUBLE)){
				av.setN(((Double)value).toString());
			}else if(type.equals(DataType.FLOAT)){
				av.setN(((Float)value).toString());
			}else if(type.equals(DataType.BIGINTEGER)){
				av.setN(((BigInteger)value).toString());
			}else if(type.equals(DataType.BIGDECIMAL)){
				av.setN(((BigDecimal)value).toString());
			} else if(type.equals(DataType.BOOLEAN)){
				av.setBOOL(((Boolean)value));
			} else if(type.equals(DataType.DATE)){
				SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			    dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
			    String strValue = dateFormatter.format((Date)value);
			    av.setS(strValue);
			} 
			return av;
		} catch (Exception e) {
			throw new DynamoDataException(e);
		}
	}
	
	/**
	 * Auxiliary function to create a String whit condition for query(ies).
	 */
	public String generateCondition(String keyParam){
		String condition = "";
		condition += key;
		condition += " ";
		if(ctype.equals(ComparisonType.EQUAL)){
			condition += "=";
		} else if(ctype.equals(ComparisonType.MINOR)){
			condition += "<";
		} else if(ctype.equals(ComparisonType.MAJOR)){
			condition += ">";
		} else if(ctype.equals(ComparisonType.LIKE)){
			condition += "like";
		}else if(ctype.equals(ComparisonType.MINOR_EQUAL)){
			condition += "<=";
		} else if(ctype.equals(ComparisonType.MAJOR_EQUAL)){
			condition += ">=";
		} 
		condition += " ";
		condition += keyParam;
		return condition;
	}
}
