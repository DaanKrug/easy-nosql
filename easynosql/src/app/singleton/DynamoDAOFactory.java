package app.singleton;

import krug.daan.easynosql.dynamodb.config.DynamoConfig;
import krug.daan.easynosql.dynamodb.dao.BaseDAO;


public class DynamoDAOFactory {
	
	private static DynamoConfig dnc;
	
	static{
		dnc = new DynamoConfig(null,null,5L,6L);
	}
	
	public static BaseDAO getBaseDAO(){
		return BaseDAO.getInstance(dnc);
	}
}
