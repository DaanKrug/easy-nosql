package app.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;

import krug.daan.easynosql.common.KeyValue;
import krug.daan.easynosql.couchdb.dto.BaseDTO;
import krug.daan.easynosql.couchdb.dto.RelationalIntegrityDTO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lightcouch.CouchDbClient;
import org.lightcouch.Response;

import app.model.CouchUser;
import app.singleton.CouchDAOFactory;

public class CouchTest {
	
	private static void printUsers(Collection<BaseDTO> users, boolean printRelateds, boolean printRelations){
		try {
			System.out.println("couch users size => " + users.size());
			for(BaseDTO dto : users){
				CouchUser u = (CouchUser)dto;
				System.out.println("couch user[id: " + u.getId() + " , name:  " + u.getName() + " , old: " + u.getOld() + " , email: " + u.getEmail() + " , rating: " + u.getRating() + "]");
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
				Collection<BaseDTO> ridtos = CouchDAOFactory.getBaseDAO().find(new RelationalIntegrityDTO(), null);
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
			CouchUser u = new CouchUser();
			u.setCreated(now);
			u.setEmail(email);
			u.setOld(1 + new Double(Math.random() * 100).intValue());
			u.setName("couch_user_" + now.getTime());
			u.setPoints(new Double(Math.random() * 100000).longValue());
			u.setRating(1D + new Double(Math.random() * 10000));
			Response response = CouchDAOFactory.getBaseDAO().saveOrUpdate(u);
			System.out.println("created couch user whit email: " + email);
			System.out.println("\t" + response.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void listUsers(){
		try {
			Collection<BaseDTO> users = CouchDAOFactory.getBaseDAO().find(new CouchUser(), null);
			printUsers(users,true,true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void deleteUser(String email){
		try {
			List<KeyValue> keyValues = new ArrayList<KeyValue>();
			keyValues.add(new KeyValue("email", email));
			CouchUser u = new CouchUser();
			u.setEmail(email);
			List<Response> responses = CouchDAOFactory.getBaseDAO().deleteAll(u, keyValues);
			if(responses.size() > 0){
				for(Response resp: responses){
					System.out.println("Deleted couch users whit email = '" + email +"': ");
					System.out.println("\t" + resp.toString());
				}
			}else{
				System.out.println("Deleted couch users whit email = '" + email +"': 0. Inexist a user whit this email.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void updateUser(String email, String name, Integer old){
		try {
			CouchUser chu = findByEmail(email);
			if(null==chu){
				System.out.println("Couch User mgu[email: " + email + "] is null");
				return;
			}
			chu.setName(name);
			chu.setOld(old);
			
			List<KeyValue> updateParameters = new ArrayList<KeyValue>();
			updateParameters.add(new KeyValue("name", name));
			updateParameters.add(new KeyValue("old", old));
			// where email = 'email' and _id = 'id'
			List<KeyValue> searchParameters = new ArrayList<KeyValue>();
			searchParameters.add(new KeyValue("email", email));
			searchParameters.add(new KeyValue("id", chu.getId()));
			List<Response> responses = CouchDAOFactory.getBaseDAO().updateAll(chu,searchParameters,updateParameters);
			if(responses.size() > 0){
				System.out.println("Updated couch user [id: " + chu.getId() + "], email setted to '" 
						+ email + "' ,name setted to '" + name + "' , old setted to '" + old + "'.");
				for(Response resp: responses){
					System.out.println("\t" + resp.toString());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static CouchUser findByEmail(String email){
		try {
			List<KeyValue> searchParameters = new ArrayList<KeyValue>();
			searchParameters.add(new KeyValue("email", email));
			Collection<BaseDTO> dtos = CouchDAOFactory.getBaseDAO().find(new CouchUser(), searchParameters);
			if(null!=dtos && dtos.size() > 0){
				return (CouchUser)dtos.iterator().next();
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
			Collection<BaseDTO> users = CouchDAOFactory.getBaseDAO().find(new CouchUser(), searchParameters);
			System.out.println("Searching couch users whit old = [" + old + "]");
			printUsers(users,false,false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void addRelatedUser(String chu1Email, String chu2Email){
		try {
			CouchUser chu1 = findByEmail(chu1Email);
			CouchUser chu2 = findByEmail(chu2Email);
			if(null==chu1){
				System.out.println("Couch User chu1[email: " + chu1Email + "] is null");
			}
			if(null==chu2){
				System.out.println("Couch User chu2[email: " + chu2Email + "] is null");
			}
			if(null!=chu1 && null!=chu2){
				chu1.addRelatedObject(chu2.getId(), new CouchUser());
				CouchDAOFactory.getBaseDAO().saveOrUpdate(chu1);
				System.out.println("Couch User chu1[id: " + chu1.getId() + "] reference to Couch User chu2[id: " + chu2.getId() + "] was added." );
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void removeRelatedUser(String chu1Email, String chu2Email){
		try {
			CouchUser chu1 = findByEmail(chu1Email);
			CouchUser chu2 = findByEmail(chu2Email);
			if(null==chu1){
				System.out.println("Couch User mgu1[email: " + chu1Email + "] is null");
			}
			if(null==chu2){
				System.out.println("Couch User mgu2[email: " + chu2Email + "] is null");
			}
			if(null!=chu1 && null!=chu2){
				chu1.removeRelatedObject(chu2.getId(), new CouchUser());
				Response response = CouchDAOFactory.getBaseDAO().saveOrUpdate(chu1);
				System.out.println("Couch User chu1[id: " + chu1.getId() + "] reference to Couch User chu2[id: " + chu2.getId() + "] was removed.");
				System.out.println("\t" + response.toString());
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
		/*
		if(true){
			try {
				CouchUser chu = findByEmail(userEmails[0]);
				System.out.println(chu.getId());
				chu.setOld(200);
				Response resp = CouchDAOFactory.getBaseDAO().saveOrUpdate(chu);
				System.out.println(resp);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return;
		}
		*/
		
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
