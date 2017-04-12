package app.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import krug.daan.easynosql.cassandradb.dto.BaseDTO;
import krug.daan.easynosql.cassandradb.dto.RelationalIntegrityDTO;
import krug.daan.easynosql.common.KeyValue;
import app.model.CassandraUser;
import app.singleton.CassandraDAOFactory;

public class CassandraTest {
	
	private static void shutdown(){
		try {
			CassandraDAOFactory.getCassandraDAO().shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void selectDatabase(String dbname){
		try {
			CassandraDAOFactory.getCassandraDAO().useDatabase(dbname);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void createDatabase(String dbname, boolean networkStrategy, 
								int replicationFactor, boolean durableWrites, boolean ignoreIfAlreadyExists){
		try {
			CassandraDAOFactory.getCassandraDAO()
						.createOrUpdateDatabase(dbname,networkStrategy,replicationFactor,durableWrites,true,ignoreIfAlreadyExists);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void updateDatabase(String dbname, boolean networkStrategy, 
			int replicationFactor, boolean durableWrites ){
		try {
			CassandraDAOFactory.getCassandraDAO()
				.createOrUpdateDatabase(dbname,networkStrategy,replicationFactor,durableWrites,false,false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void dropDatabase(String dbname){
		try {
			CassandraDAOFactory.getCassandraDAO().dropDatabase(dbname);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void createTable(BaseDTO dto, boolean ignoreIfAlreadyExists){
		try {
			CassandraDAOFactory.getCassandraDAO().createTable(dto,ignoreIfAlreadyExists);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void dropTable(BaseDTO dto){
		try {
			CassandraDAOFactory.getCassandraDAO().dropTable(dto);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void alterTable(BaseDTO dto, List<KeyValue> keyValues, boolean toDrop){
		try {
			CassandraDAOFactory.getCassandraDAO().alterTable(dto,keyValues,toDrop);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void alterIndex(BaseDTO dto, String attrName, String indexName, boolean toDrop){
		try {
			CassandraDAOFactory.getCassandraDAO().alterIndex(dto, attrName, indexName, toDrop);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Method to test the internal DATABASEs creation mechanism.
	 * No intended to be useful for general purposes.
	 */
	private static void databaseTest(String dbName){
		boolean ignoreIfAlreadyExists = true;
		createDatabase(dbName,false,1,true,ignoreIfAlreadyExists);
		createDatabase(dbName,false,1,true,ignoreIfAlreadyExists);
		updateDatabase(dbName,true,3,true);
		updateDatabase(dbName,true,5,false);
		selectDatabase(dbName);
		dropDatabase(dbName);
		updateDatabase(dbName + "x",true,5,false);
		selectDatabase(dbName + "x");
		dropDatabase(dbName + "x");
		shutdown();
	}
	
	/**
	 * Method to test the internal TABLEs creation mechanism.
	 * No intended to be useful for general purposes.
	 */
	private static void tablesTest(){
		createTable(new CassandraUser(),true);
	    createTable(new CassandraUser(),true);
	    createTable(new CassandraUser(),true);
	    //
	    List<KeyValue> kvAdd = new ArrayList<KeyValue>();
	    kvAdd.add(new KeyValue("AddtionalColumn1",Integer.class));
	    kvAdd.add(new KeyValue("AddtionalColumn2",String.class));
	    kvAdd.add(new KeyValue("AddtionalColumn3",Double.class));
	    kvAdd.add(new KeyValue("AddtionalColumn4",java.util.Date.class));
	    //add columns
	    alterTable(new CassandraUser(),kvAdd,false);
	    alterTable(new CassandraUser(),kvAdd,false);
	    // add a index
	    alterIndex(new CassandraUser(),"AddtionalColumn1","idx_AddtionalColumn1",false);
	    // drop a index
	    alterIndex(new CassandraUser(),"AddtionalColumn1","idx_AddtionalColumn1",true);
	    // add a index invalid
	    alterIndex(new CassandraUser(),"AddtionalColumn20","idx_AddtionalColumn20",false);
	    // drop a index invalid
	    alterIndex(new CassandraUser(),"AddtionalColumn20","idx_AddtionalColumn20",true);
	    //drop columns
	    alterTable(new CassandraUser(),kvAdd,true);
	    alterTable(new CassandraUser(),kvAdd,true);
	    //
	    dropTable(new CassandraUser());
	    dropTable(new CassandraUser());
	    shutdown();
	}
	
	
	private static void printUsers(Collection<BaseDTO> users, boolean printRelateds, boolean printRelations){
		try {
			System.out.println("cassandra users size => " + users.size());
			for(BaseDTO dto : users){
				CassandraUser u = (CassandraUser)dto;
				System.out.println("cassandra user[id: " + u.getId() + " , name:  " + u.getName() + " , old: " + u.getOld() + " , email: " + u.getEmail() + " , rating: " + u.getRating() + "]");
				if(printRelateds){
					if(null!=u.getRelatedObjectsIds()){
						System.out.println("\trelated objects id size => " + u.getRelatedObjectsIds().size());
						for(KeyValue keyValue: u.getRelatedObjectsIds()){
							System.out.println("\t\tclass => " + keyValue.getKey() + " _id: " + keyValue.getValue());
						}
					}
					if(null!=u.getRelatedObjects()){
						System.out.println("\trelated objects size => " + u.getRelatedObjects().size());
						for(KeyValue keyValue: u.getRelatedObjects()){
							BaseDTO relatedDTO = (BaseDTO)keyValue.getValue();
							System.out.println("\t\tclass => " + keyValue.getKey() + " relatedDTO _id: " + relatedDTO.getId());
						}
					}
				}
			}
			if(printRelations){
				Collection<BaseDTO> ridtos = CassandraDAOFactory.getBaseDAO().find(new RelationalIntegrityDTO(), null);
				System.out.println("relational integrities size => " + ridtos.size());
				for(BaseDTO dto : ridtos){
					RelationalIntegrityDTO ridto = (RelationalIntegrityDTO) dto;
					System.out.println("\t\tid: " + ridto.getId() + " Owner: " + ridto.getOwnerId() + " Related: " + ridto.getRelatedId());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void createUser(String email){
		try {
			java.util.Date now = new java.util.Date();
			CassandraUser u = new CassandraUser();
			u.setCreated(now);
			u.setEmail(email);
			u.setOld(1 + new Double(Math.random() * 100).intValue());
			u.setName("cassandra_user_" + now.getTime());
			u.setPoints(new Double(Math.random() * 100000).longValue());
			u.setRating(1D + new Double(Math.random() * 10000));
			CassandraDAOFactory.getBaseDAO().saveOrUpdate(u);
			System.out.println("created cassandra user with email: " + email);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void listUsers(){
		try {
			Collection<BaseDTO> users = CassandraDAOFactory.getBaseDAO().find(new CassandraUser(), null);
			printUsers(users,true,true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void deleteUser(String email){
		try {
			List<KeyValue> keyValues = new ArrayList<KeyValue>();
			keyValues.add(new KeyValue("email", email));
			CassandraUser u = new CassandraUser();
			u.setEmail(email);
			Integer deleted = CassandraDAOFactory.getBaseDAO().deleteAll(u, keyValues);
			if(deleted > 0){
				System.out.println("Deleted cassandra users whit email = '" + email +"': ");
				System.out.println("\t" + deleted);
			}else{
				System.out.println("Deleted cassandra users whit email = '" + email +"': 0. Inexist a user whit this email.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void updateUser(String email, String name, Integer old){
		try {
			CassandraUser csu = findByEmail(email);
			if(null==csu){
				System.out.println("Cassandra User csu[email: " + email + "] is null");
				return;
			}
			csu.setName(name);
			csu.setOld(old);
			
			List<KeyValue> updateParameters = new ArrayList<KeyValue>();
			updateParameters.add(new KeyValue("name", name));
			updateParameters.add(new KeyValue("old", old));
			// where email = 'email' and id = 'id'
			List<KeyValue> searchParameters = new ArrayList<KeyValue>();
			searchParameters.add(new KeyValue("email", email));
			searchParameters.add(new KeyValue("id", csu.getId()));
			Integer updated = CassandraDAOFactory.getBaseDAO().updateAll(csu,searchParameters,updateParameters);
			if(updated > 0){
				System.out.println("Updated " + updated + " cassandra user(s) whit [id: " + csu.getId() + ", email: " + email +"] " 
						+ "name setted to '" + name + "' , old setted to '" + old + "'.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static CassandraUser findByEmail(String email){
		try {
			List<KeyValue> searchParameters = new ArrayList<KeyValue>();
			searchParameters.add(new KeyValue("email", email));
			Collection<BaseDTO> dtos = CassandraDAOFactory.getBaseDAO().find(new CassandraUser(), searchParameters);
			if(null!=dtos && dtos.size() > 0){
				return (CassandraUser)dtos.iterator().next();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private static void printByOld(Integer old){
		try {
			List<KeyValue> searchParameters = new ArrayList<KeyValue>();
			searchParameters.add(new KeyValue("old", old));
			Collection<BaseDTO> users = CassandraDAOFactory.getBaseDAO().find(new CassandraUser(), searchParameters);
			System.out.println("Searching cassandra users whit old = [" + old + "]");
			printUsers(users,false,false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void addRelatedUser(String csu1Email, String csu2Email){
		try {
			CassandraUser csu1 = findByEmail(csu1Email);
			CassandraUser csu2 = findByEmail(csu2Email);
			if(null==csu1){
				System.out.println("Cassandra User csu1[email: " + csu1Email + "] is null");
			}
			if(null==csu2){
				System.out.println("Cassandra User csu2[email: " + csu2Email + "] is null");
			}
			if(null!=csu1 && null!=csu2){
				csu1.addRelatedObject(csu2.getId(), new CassandraUser());
				CassandraDAOFactory.getBaseDAO().saveOrUpdate(csu1);
				System.out.println("Cassandra User csu1[id: " + csu1.getId() + "] reference to Cassandra User csu2[id: " + csu2.getId() + "] was added." );
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void removeRelatedUser(String csu1Email, String chu2Email){
		try {
			CassandraUser csu1 = findByEmail(csu1Email);
			CassandraUser csu2 = findByEmail(chu2Email);
			if(null==csu1){
				System.out.println("CassandraUser User csu1[email: " + csu1Email + "] is null");
			}
			if(null==csu2){
				System.out.println("CassandraUser User csu2[email: " + chu2Email + "] is null");
			}
			if(null!=csu1 && null!=csu2){
				csu1.removeRelatedObject(csu2.getId(), new CassandraUser());
				CassandraDAOFactory.getBaseDAO().saveOrUpdate(csu1);
				System.out.println("Cassandra User csu1[id: " + csu1.getId() + "] reference to Cassandra User csu2[id: " + csu2.getId() + "] was removed.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
		// databaseTest("db1");
		// tablesTest();
		
		String[] userEmails = new String[]{
				"alpha@star.com",
				"bravo@star.com",
				"charlie@star.com",
				"delta@star.com",
				"echo@star.com",
				"fox@star.com",
				"lima@star.com",
				"mango@star.com",
				"tango@star.com",
				"zulu@star.com"
		};
		
		String[] userEmailsNull = new String[]{
				"xalpha@star.com",
				"xbravo@star.com",
				"xcharlie@star.com",
				"xdelta@star.com",
				"xecho@star.com",
				"xfox@star.com",
				"xlima@star.com",
				"xmango@star.com",
				"xtango@star.com",
				"xzulu@star.com"
		};
		
		String[] userNames = new String[]{
				"Alpha",
				"Bravo",
				"Charlie",
				"Delta",
				"Echo",
				"Fox",
				"Lima",
				"Mango",
				"Tango",
				"Zulu"
		};
		
		Integer[] userOlds = new Integer[]{30,40,30,20,10,20,14,17,22,20};
		Integer[] userOldsSearch = new Integer[]{10,14,17,20,22,30,40};
		
		//
		int testNumber = 0;
		
		testNumber ++;
		System.out.println("###### [begin]test "+ testNumber +" create users by emails ############");
		for(String email: userEmails){
			createUser(email);
		}
		listUsers();
		System.out.println("###### [end] test "+ testNumber +" create users by emails ############\n\n");
		
		//
		testNumber ++;
		System.out.println("###### [begin]test "+ testNumber +" update users ############");
		int pos = 0;
		for(String email: userEmails){
			updateUser(email, userNames[pos], userOlds[pos]);
			pos++;
		}
		listUsers();
		System.out.println("###### [end] test "+ testNumber +" update users ############\n\n");
		
		
		//
		testNumber ++;
		System.out.println("###### [begin]test "+ testNumber +" search users by old ############");
		pos = 0;
		for(Integer old: userOldsSearch){
			printByOld(old);
		}
		System.out.println("###### [end] test "+ testNumber +" search users by old ############\n\n");
		
		//
		testNumber ++;
		System.out.println("###### [begin]test "+ testNumber +" associate users ############");
		addRelatedUser(userEmails[0], userEmails[1]);
		addRelatedUser(userEmails[0], userEmails[2]);
		addRelatedUser(userEmails[1], userEmails[2]);
		addRelatedUser(userEmailsNull[0], userEmails[2]);//user 1 dont exist
		addRelatedUser(userEmails[0], userEmailsNull[2]);//user 2 dont exist
		addRelatedUser(userEmailsNull[0], userEmailsNull[2]);//both user 1 and user 2 dont exist
		listUsers();
		System.out.println("###### [end] test "+ testNumber +" associate users  ############\n\n");
		
		//
		testNumber ++;
		System.out.println("###### [begin]test "+ testNumber +" try delete a related user ############");
		deleteUser(userEmails[1]);
		deleteUser(userEmails[2]);
		listUsers();
		System.out.println("###### [end] test "+ testNumber +" try delete a related user  ############\n\n");
		
		//
		testNumber ++;
		System.out.println("###### [begin]test "+ testNumber +" remove associated users ############");
		removeRelatedUser(userEmails[0], userEmails[1]);
		removeRelatedUser(userEmails[0], userEmails[2]);
		removeRelatedUser(userEmails[1], userEmails[2]);
		removeRelatedUser(userEmailsNull[0], userEmails[2]);//user 1 dont exist
		removeRelatedUser(userEmails[0], userEmailsNull[2]);//user 2 dont exist
		removeRelatedUser(userEmailsNull[0], userEmailsNull[2]);//both user 1 and user 2 dont exist
		listUsers();
		System.out.println("###### [end] test "+ testNumber +" remove associated users  ############\n\n");
		
		
		//
		testNumber ++;
		System.out.println("###### [begin]test "+ testNumber +" delete users by emails ############");
		for(String email: userEmails){
			deleteUser(email);
		}
		listUsers();
		System.out.println("###### [end] test "+ testNumber +" delete users by emails ############\n\n");
		
		//
		testNumber ++;
		System.out.println("###### [begin]test "+ testNumber +" try delete inexistent users ############");
		deleteUser(userEmailsNull[0]);
		deleteUser(userEmailsNull[1]);
		System.out.println("###### [end] test "+ testNumber +" try delete inexistent users ############\n\n");
		
	}

}
