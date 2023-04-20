package cs505finaltemplate.graphDB;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
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

        if (patient == null) {
            patient = db.createVertexClass("patient");
        }

        if (patient.getProperty("patient_mrn") == null) {
            patient.createProperty("patient_mrn", OType.STRING);
            patient.createProperty("testing_id", OType.INTEGER);

            patient.createProperty("patient_name", OType.STRING);

            patient.createProperty("patient_zipcode", OType.INTEGER);

            patient.createProperty("patient_status", OType.INTEGER);
            patient.createIndex("patient_name_index", OClass.INDEX_TYPE.NOTUNIQUE, "patient_mrn");
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

    private static OVertex createPatient( int testing_id, String patient_name, String patient_mrn, int patient_zipcode, int patient_status) {
        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("covid_data", "root", "rootpwd");
        System.out.println("****************************************************************************************************");

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
        
        if(rs==null){
            System.out.println("****rs is "+rs);
        }
        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println("contact: " + item.getProperty("patient_mrn"));
            if(item.getProperty("patient_mrn")!=null){
                arr.add(item.getProperty("patient_mrn"));
            }
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        return arr;

    }

    public ArrayList<String> getPossibleContacts(ODatabaseSession db, String patient_mrn){

        //String q = "match (e:Event)<-[r:attend]-(p:Patient) call { with p match (p)<-[r1:contact]->(p2) where p2.mrn=$mrn return p as otherpat } return otherpat, e";
        String query = "SELECT patient_mrn FROM (TRAVERSE inE('contact_with'), outE('contact_with')) FROM (TRAVERSE outE('contact_with') FROM (select from patient where patient_mrn = '"+patient_mrn+"'))";
            
        OResultSet rs = db.query(query);
        ArrayList<String> arr = new ArrayList<String>();
        System.out.println("****rs is "+rs.next());
        if(rs==null){
            System.out.println("****rs is "+rs);
        }
        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println("contact: " + item.getProperty("patient_mrn"));
            if(item.getProperty("patient_mrn")!=null){
                arr.add(item.getProperty("patient_mrn"));
            }
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        return arr;
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
        // db.close();
        orient.close();

    }


    public static void add_rec(int testing_id, String patient_name, String patient_mrn, int patient_zipcode, int patient_status, List<String> contact_list, List<String> event_list){
		        
       
        createPatient(testing_id, patient_name, patient_mrn, patient_zipcode, patient_status);

        String p1 = get_patient_rid(patient_mrn);

        for(String c:contact_list){
            String p2 = get_patient_rid(c);
            if(!p2.equals("")){
                make_edge(p1,p2);
                make_edge(p2,p1);
            }
       
        }

    }

    private void clearDB(ODatabaseSession db) {

        String query = "DELETE VERTEX FROM patient";
        db.command(query);

    }

}
