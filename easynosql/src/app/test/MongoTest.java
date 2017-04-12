package app.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import krug.daan.easynosql.common.KeyValue;
import krug.daan.easynosql.mongodb.dto.BaseDTO;
import krug.daan.easynosql.mongodb.dto.RelationalIntegrityDTO;

import org.bson.types.ObjectId;

import app.model.MongoUser;
import app.singleton.MongoDAOFactory;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class MongoTest {
	
	private static void printUsers(Collection<BaseDTO> users, boolean printRelateds, boolean printRelations){
		try {
			System.out.println("mongo users size => " + users.size());
			for(BaseDTO dto : users){
				MongoUser u = (MongoUser)dto;
				System.out.println("mongo user[id: " + u.getId() + " , name:  " + u.getName() + " , email: " + u.getEmail() + " , rating: " + u.getRating() + "]");
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
				Collection<BaseDTO> ridtos = MongoDAOFactory.getBaseDAO().find(new RelationalIntegrityDTO(), null);
				System.out.println("relational integrities size => " + ridtos.size());
				for(BaseDTO dto : ridtos){
					RelationalIntegrityDTO ridto = (RelationalIntegrityDTO) dto;
					System.out.println("\t\tOwner: " + ridto.getOwnerId() + " Related: " + ridto.getRelatedId());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void createUser(String email){
		try {
			java.util.Date now = new java.util.Date();
			MongoUser u = new MongoUser();
			u.setCreated(now);
			u.setEmail(email);
			u.setOld(1 + new Double(Math.random() * 100).intValue());
			u.setName("mongo_user_" + now.getTime());
			u.setPoints(new Double(Math.random() * 100000).longValue());
			u.setRating(1D + new Double(Math.random() * 10000));
			MongoDAOFactory.getBaseDAO().saveOrUpdate(u);
			System.out.println("created mongo user whit email: " + email);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void listUsers(){
		try {
			Collection<BaseDTO> users = MongoDAOFactory.getBaseDAO().find(new MongoUser(), null);
			printUsers(users,true,true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void deleteUser(String email){
		try {
			List<KeyValue> keyValues = new ArrayList<KeyValue>();
			keyValues.add(new KeyValue("email", email));
			MongoUser u = new MongoUser();
			u.setEmail(email);
			DeleteResult dr = MongoDAOFactory.getBaseDAO().deleteAll(u, keyValues);
			System.out.println("Deleted mongo users whit email = '" + email +"' : " + dr.getDeletedCount());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void updateUser(String email, String name, Integer old){
		try {
			MongoUser mgu = findByEmail(email);
			if(null==mgu){
				System.out.println("Mongo User mgu[email: " + email + "] is null");
				return;
			}
			mgu.setName(name);
			mgu.setOld(old);
			// where email = 'email' and _id = 'id'
			List<KeyValue> keyValues = new ArrayList<KeyValue>();
			keyValues.add(new KeyValue("email", email));
			keyValues.add(new KeyValue("_id", mgu.getId()));
			UpdateResult ur = MongoDAOFactory.getBaseDAO().update(mgu, keyValues);
			System.out.println("Updated mongo user [id: " + mgu.getId() + "], email setted to '" 
					+ email + "' ,name setted to '" + name + "' , old setted to '" + old + "'. Total mongo users updated: "  + ur.getModifiedCount());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static MongoUser findByEmail(String email){
		try {
			List<KeyValue> searchParameters = new ArrayList<KeyValue>();
			searchParameters.add(new KeyValue("email", email));
			Collection<BaseDTO> dtos = MongoDAOFactory.getBaseDAO().find(new MongoUser(), searchParameters);
			if(null!=dtos && dtos.size() > 0){
				return (MongoUser)dtos.iterator().next();
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
			Collection<BaseDTO> users = MongoDAOFactory.getBaseDAO().find(new MongoUser(), searchParameters);
			System.out.println("Searching mongo users whit old = [" + old + "]");
			printUsers(users,false,false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void addRelatedUser(String mgu1Email, String mgu2Email){
		try {
			MongoUser mgu1 = findByEmail(mgu1Email);
			MongoUser mgu2 = findByEmail(mgu2Email);
			if(null==mgu1){
				System.out.println("Mongo User mgu1[email: " + mgu1Email + "] is null");
			}
			if(null==mgu2){
				System.out.println("Mongo User mgu2[email: " + mgu2Email + "] is null");
			}
			if(null!=mgu1 && null!=mgu2){
				mgu1.addRelatedObject(mgu2.getId(), new MongoUser());
				MongoDAOFactory.getBaseDAO().saveOrUpdate(mgu1);
				System.out.println("Mongo User mgu1[id: " + mgu1.getId().toHexString() + "] reference to Mongo User mgu2[id: " + mgu2.getId().toHexString() + "] was added." );
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void removeRelatedUser(String mgu1Email, String mgu2Email){
		try {
			MongoUser mgu1 = findByEmail(mgu1Email);
			MongoUser mgu2 = findByEmail(mgu2Email);
			if(null==mgu1){
				System.out.println("Mongo User mgu1[email: " + mgu1Email + "] is null");
			}
			if(null==mgu2){
				System.out.println("Mongo User mgu2[email: " + mgu2Email + "] is null");
			}
			if(null!=mgu1 && null!=mgu2){
				mgu1.removeRelatedObject(mgu2.getId(), new MongoUser());
				UpdateResult ur = MongoDAOFactory.getBaseDAO().update(mgu1);
				System.out.println("Mongo User mgu1[id: " + mgu1.getId().toHexString() + "] reference to Mongo User mgu2[id: " + mgu2.getId().toHexString() + "] was removed. Updated objects: " + ur.getModifiedCount() );
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
