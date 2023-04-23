package cs505finaltemplate.graphDB;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
//import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import net.minidev.json.JSONObject;

import java.util.*;

public class GraphDBEngine {


    
    public GraphDBEngine() {

        //launch a docker container for orientdb, don't expect your data to be saved unless you configure a volume
        //docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:3.0.0

        //use the orientdb dashboard to create a new database
        //see class notes for how to use the dashboard


        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("covid_data", "root", "rootpwd");

        clearDB(db);

        //create classes
        OClass patient = db.getClass("patient");
        OClass event = db.getClass("event");

        if (patient == null) {
            patient = db.createVertexClass("patient");
        }

        if (patient.getProperty("patient_mrn") == null) {
            patient.createProperty("patient_mrn", OType.STRING);
            patient.createProperty("testing_id", OType.INTEGER);

            patient.createProperty("patient_name", OType.STRING);

            patient.createProperty("patient_zipcode", OType.INTEGER);

            patient.createProperty("patient_status", OType.INTEGER);
            //patient.createIndex("patient_name_index", OClass.INDEX_TYPE.NOTUNIQUE, "patient_mrn");
        }

        if (event == null) {
            event = db.createVertexClass("event");
        }

        if (event.getProperty("event_id") == null) {
            event.createProperty("event_id", OType.STRING);
        }

        if (db.getClass("contact_with") == null) {
            db.createEdgeClass("contact_with");
	    }	

        if (db.getClass("event_with") == null) {
            db.createEdgeClass("event_with");
        }

        db.close();
        orient.close();

    }

    private static OVertex createPatient(int testing_id, String patient_name, String patient_mrn, int patient_zipcode, int patient_status) {
        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("covid_data", "root", "rootpwd");
        // System.out.println("****************************************************************************************************");

        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", patient_mrn);
        result.setProperty("testing_id", testing_id);
        result.setProperty("patient_name", patient_name);
        result.setProperty("patient_zipcode", patient_zipcode);
        result.setProperty("patient_status", patient_status);
        result.save();
       
        db.close();
        orient.close();

        return result;
    }

    public  ArrayList<String> getContacts(ODatabaseSession db, String patient_mrn){

        String query = "select patient_mrn from (TRAVERSE inE('contact_with'), outE('contact_with'), inV(), outV() FROM (select from patient where patient_mrn = '"+patient_mrn+"' ) WHILE $depth <= 2)";
            
        OResultSet rs = db.query(query);
        ArrayList<String> arr = new ArrayList<String>();
        while (rs.hasNext()) {
            OResult item = rs.next();
            if(item.getProperty("patient_mrn")!=null){
                arr.add(item.getProperty("patient_mrn"));
            }
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        return arr;

    }

    public JSONObject getPossibleContacts(ODatabaseSession db, String patient_mrn){

        String query = "select event_id from (TRAVERSE inE('event_with'), outE('event_with'), inV(), outV() FROM (select from patient where patient_mrn = '"+patient_mrn+"' ) WHILE $depth <= 2)";
            
        OResultSet rs = db.query(query);
        ArrayList<String> arr = new ArrayList<>();
        
        JSONObject event = new JSONObject();
        System.out.println("here------------------"+rs.next());
        while (rs.hasNext()) { 
            OResult item = rs.next();
            System.out.println("has event id ------------------");
            if(item.getProperty("event_id")!=null){ 
                
                arr.add(item.getProperty("event_id"));
                System.out.println("-----Event ID===================="+arr);
                String query2 = "select patient_mrn from (TRAVERSE inE('event_with'), outE('event_with'), inV(), outV() FROM (select from event where event_id = '"+item.getProperty("event_id")+"') WHILE $depth <= 2)"; 
                OResultSet rs2 = db.query(query2); 
                ArrayList<String> mrn_arr = new ArrayList<String>();
                while (rs2.hasNext()) { 
                    OResult item2 = rs2.next(); 
                    
                    if(item2.getProperty("patient_mrn")!=null && !patient_mrn.equals(item2.getProperty("patient_mrn"))){ 
                        mrn_arr.add(item2.getProperty("patient_mrn")); 
                    }
                    event.put(item.getProperty("event_id"),mrn_arr); 
                } 
                
                rs2.close(); 
            } 
    
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        return event;

    }

   private static String get_patient_rid(String c){
        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("covid_data", "root", "rootpwd");
        String sql = "SELECT * from `patient` WHERE `patient_mrn` = '" + c + "'";
       

        OResultSet rs = db.query(sql);
        String s="";
        while (rs.hasNext()) {
            OResult item = rs.next();
            // System.out.println("contact: " + item.getProperty("@rid"));
            s = s+item.getProperty("@rid");
        }
       
        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!

        db.close();
        orient.close();
        return s;
    }

    public static void make_edge(String p1,String p2){
        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("covid_data", "root", "rootpwd");
       
        String sql = "CREATE EDGE contact_with FROM "+p1+" TO "+p2;
        if(!p1.equals(p2)){

            OResultSet rs = db.command(sql);
            rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        }
        db.close();
        orient.close();

    }

    public static void createEdgeClass( String p1,String p2, String type){
        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("covid_data", "root", "rootpwd");
       
        String sql = "CREATE EDGE "+ type +" FROM "+p1+" TO "+p2;
        if(!p1.equals(p2)){

            OResultSet rs = db.command(sql);
            rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        }
        db.close();
        orient.close();
    }
    public static String get_event_rid(String e){

        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("covid_data", "root", "rootpwd");
        String sql = "SELECT from event WHERE event_id='"+e+"'";



        OResultSet rs = db.query(sql);
        String s="";
        if(rs.hasNext()){
            while (rs.hasNext()) {
                OResult item = rs.next();
                // System.out.println("contact: " + item.getProperty("@rid"));
                s = s+item.getProperty("@rid");
            }
        }
        else{
            OVertex result = db.newVertex("event");
            result.setProperty("event_id", e);
            result.save();
            s = s+result.getIdentity();
        }

        db.close();
        orient.close();
        return s;

    }

    public static void add_rec(int testing_id, String patient_name, String patient_mrn, int patient_zipcode, int patient_status, List<String> contact_list, List<String> event_list){
        createPatient(testing_id, patient_name, patient_mrn, patient_zipcode, patient_status);
        System.out.println("Add_rec here------------------");
        String p1 = get_patient_rid(patient_mrn);
        String p2 = null;
        for(String c:contact_list){
            p2 = get_patient_rid(c);
            if(!p2.equals("")){
                createEdgeClass(p1,p2,"contact_with");
                createEdgeClass(p2,p1,"contact_with");
            }
    
        }
        for(String el:event_list){
            
            p2 = get_event_rid(el);

            if(!p2.equals("")){
                createEdgeClass(p1,p2,"event_with");
                createEdgeClass(p2,p1,"event_with");
            }
    
        }

    }

    public static void clearDB(ODatabaseSession db) {

        String query1 = "DELETE VERTEX FROM patient";
        db.command(query1);
        String query2 = "DELETE VERTEX FROM event";
        db.command(query2);

    }

}
