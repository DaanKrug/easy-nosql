package app.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import krug.daan.easynosql.common.KeyValue;
import krug.daan.easynosql.dynamodb.aws.AwsKeyValue;
import krug.daan.easynosql.dynamodb.dao.AwsTableDAO;
import krug.daan.easynosql.dynamodb.dto.BaseDTO;
import krug.daan.easynosql.dynamodb.dto.RelationalIntegrityDTO;
import krug.daan.easynosql.dynamodb.type.ComparisonType;
import krug.daan.easynosql.dynamodb.type.DataType;
import app.model.DynamoUser;
import app.singleton.DynamoDAOFactory;


public class DynamoTest {
	
	private static void printUsers(Collection<BaseDTO> users, boolean printRelateds, boolean printRelations){
		try {
			System.out.println("Dynamo users size => " + users.size());
			for(BaseDTO dto : users){
				DynamoUser u = (DynamoUser)dto;
				System.out.println("Dynamo user[id: " + u.getId() + " , name:  " + u.getName() + " , old: " + u.getYearsOld() + " , email: " + u.getEmail() + " , rating: " + u.getRating() + "]");
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
				Collection<BaseDTO> ridtos = DynamoDAOFactory.getBaseDAO().findAll(new RelationalIntegrityDTO(),false);
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
			DynamoUser u = new DynamoUser();
			u.setCreated(now);
			u.setEmail(email);
			u.setYearsOld(1 + new Double(Math.random() * 100).intValue());
			u.setName("dynamo_user_" + now.getTime());
			u.setPoints(new Double(Math.random() * 100000).longValue());
			u.setRating(1D + new Double(Math.random() * 10000));
			DynamoDAOFactory.getBaseDAO().saveOrUpdate(u);
			System.out.println("created dynamo user whit email: " + email);
			//System.out.println("\t" + response.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void listUsers(){
		try {
			Collection<BaseDTO> users = DynamoDAOFactory.getBaseDAO().findAll(new DynamoUser(),true);
			printUsers(users,true,true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void deleteUser(String email){
		try {
			List<AwsKeyValue> keyValues = new ArrayList<AwsKeyValue>();
			keyValues.add(new AwsKeyValue("email", email,DataType.STRING,ComparisonType.EQUAL));
			DynamoUser u = new DynamoUser();
			u.setEmail(email);
			long deletedObjects = DynamoDAOFactory.getBaseDAO().deleteAll(u, keyValues);
			if(deletedObjects > 0){
				System.out.println("Deleted " + deletedObjects + " dynamo users whit email = '" + email +"'");
			}else{
				System.out.println("Deleted 0 dynamo users whit email = '" + email +"'. Inexist a user whit this email.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void updateUser(String email, String name, Integer old){
		try {
			DynamoUser dyu = findByEmail(email);
			if(null==dyu){
				System.out.println("Dynamo User dyu[email: " + email + "] is null");
				return;
			}
			dyu.setName(name);
			dyu.setYearsOld(old);
			
			List<KeyValue> updateParameters = new ArrayList<KeyValue>();
			updateParameters.add(new KeyValue("name", name));
			updateParameters.add(new KeyValue("yearsOld", old));
			// where email = 'email' and id = 'id'
			List<AwsKeyValue> searchParameters = new ArrayList<AwsKeyValue>();
			searchParameters.add(new AwsKeyValue("email", email,DataType.STRING,ComparisonType.EQUAL));
			searchParameters.add(new AwsKeyValue("id", dyu.getId(),DataType.STRING,ComparisonType.EQUAL));
			long updatedObjects = DynamoDAOFactory.getBaseDAO().updateAll(dyu,searchParameters,updateParameters);
			if(updatedObjects > 0){
				System.out.println("Updated " + updatedObjects + " dynamo user(s): Email setted to '" 
						+ email + "' ,name setted to '" + name + "' , old setted to '" + old + "'.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static DynamoUser findByEmail(String email){
		try {
			List<AwsKeyValue> searchParameters = new ArrayList<AwsKeyValue>();
			searchParameters.add(new AwsKeyValue("email", email,DataType.STRING,ComparisonType.EQUAL));
			return (DynamoUser)DynamoDAOFactory.getBaseDAO().find(new DynamoUser(), searchParameters,false,false);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private static void printByOld(Integer old){
		try {
			System.out.println("Searching dynamo users whit old = [" + old + "]");
			List<AwsKeyValue> searchParameters = new ArrayList<AwsKeyValue>();
			searchParameters.add(new AwsKeyValue("yearsOld", old,DataType.INTEGER,ComparisonType.EQUAL));
			Collection<BaseDTO> users = DynamoDAOFactory.getBaseDAO().findAll(new DynamoUser(), searchParameters, -1, false, false);
			printUsers(users,false,false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void printByOldInterval(Integer oldBegin,Integer oldEnd){
		try {
			System.out.println("Searching dynamo users whit old between [" + oldBegin + "] and [" + oldEnd + "]");
			List<AwsKeyValue> searchParameters = new ArrayList<AwsKeyValue>();
			searchParameters.add(new AwsKeyValue("yearsOld", oldBegin,DataType.INTEGER,ComparisonType.MAJOR_EQUAL));
			searchParameters.add(new AwsKeyValue("yearsOld", oldEnd,DataType.INTEGER,ComparisonType.MINOR_EQUAL));
			Collection<BaseDTO> users = DynamoDAOFactory.getBaseDAO().findAll(new DynamoUser(), searchParameters, -1, false, false);
			printUsers(users,false,false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void addRelatedUser(String dyu1Email, String dyu2Email){
		try {
			DynamoUser dyu1 = findByEmail(dyu1Email);
			DynamoUser dyu2 = findByEmail(dyu2Email);
			if(null==dyu1){
				System.out.println("Dynamo User dyu1[email: " + dyu1Email + "] is null");
			}
			if(null==dyu2){
				System.out.println("Dynamo User dyu2[email: " + dyu2Email + "] is null");
			}
			if(null!=dyu1 && null!=dyu2){
				dyu1.addRelatedObject(dyu2.getId(), new DynamoUser());
				DynamoDAOFactory.getBaseDAO().saveOrUpdate(dyu1);
				System.out.println("Dynamo User dyu1[id: " + dyu1.getId() + "] reference to Dynamo User dyu2[id: " + dyu2.getId() + "] was added." );
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void removeRelatedUser(String dyu1Email, String dyu2Email){
		try {
			DynamoUser dyu1 = findByEmail(dyu1Email);
			DynamoUser dyu2 = findByEmail(dyu2Email);
			if(null==dyu1){
				System.out.println("Dynamo User dyu1[email: " + dyu1Email + "] is null");
			}
			if(null==dyu2){
				System.out.println("Dynamo User dyu2[email: " + dyu2Email + "] is null");
			}
			if(null!=dyu1 && null!=dyu2){
				dyu1.removeRelatedObject(dyu2.getId(), new DynamoUser());
				DynamoDAOFactory.getBaseDAO().saveOrUpdate(dyu1);
				System.out.println("Dynamo User dyu1[id: " + dyu1.getId() + "] reference to Dynamo User dyu2[id: " + dyu2.getId() + "] was removed.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
		
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
		
		//clear table ... 
		try {
			DynamoDAOFactory.getBaseDAO().getAwsTableDAO().deleteTable(new DynamoUser());
		} catch (Exception e) {
			e.printStackTrace();
		}
		//clear table ... 
		try {
			DynamoDAOFactory.getBaseDAO().getAwsTableDAO().deleteTable(new RelationalIntegrityDTO());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
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
		System.out.println("###### [begin]test "+ testNumber +" search users by old interval ############");
		pos = 0;
		for(Integer old: userOldsSearch){
			printByOldInterval(old-7,old+7);
		}
		System.out.println("###### [end] test "+ testNumber +" search users by old interval ############\n\n");
		
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
