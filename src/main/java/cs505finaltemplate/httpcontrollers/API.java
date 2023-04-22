package cs505finaltemplate.httpcontrollers;

import com.google.gson.Gson;
import cs505finaltemplate.Launcher;
import cs505finaltemplate.graphDB.GraphDBEngine;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    public API() {
        gson = new Gson();
    }


    @GET
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getteam() {
        String responseString = "{}";
        try {
            System.out.println("Team Alpha Bravo Reporting on Covid situation in Kentucky.");
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("team_name", "Team Alpha Bravo");
            responseMap.put("Team_members_sids", "[12602303,12598195]");
            responseMap.put("app_status_code","1");

            responseString = gson.toJson(responseMap);


        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    // CEP APIs'
	//alertlist
    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAlertList() {
        String responseString = "{}";
        Map<String, Object> res = new HashMap<String, Object>();
        try {
            int status = (Launcher.common.size()>=2)?1:0;
            res.put("state_status", status);
            responseString = gson.toJson(res);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
	
	//ziplertlist
	@GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getZipAlert(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {

            //generate a response
            Map<String,Set<Integer>> responseMap = new HashMap<>();
            responseMap.put("ziplist",Launcher.common);
            responseString = gson.toJson(responseMap);


        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
	
	// Operational Functions
	
	//curl --header "X-Auth-API-key:1234" "http://maal281.cs.uky.edu:8082/api/checkmydatabase"

    @GET
    @Path("/checkmydatabase")
    @Produces(MediaType.APPLICATION_JSON)
    public Response checkMyEndpoint() {
        String responseString = "{}";

        try {

            //get remote ip address from request
            String remoteIP = request.get().getRemoteAddr();
            //get the timestamp of the request
            long access_ts = System.currentTimeMillis();
            System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);

            Map<String,String> responseMap = new HashMap<>();
            if(Launcher.dbEngine.databaseExist("team_2_database")) {
                if(Launcher.dbEngine.tableExist("hospitaldata")) {
                    responseMap.put("success", Boolean.TRUE.toString());
                    responseMap.put("status_desc","hospitaldata table exists");
                } else {
                    responseMap.put("success", Boolean.FALSE.toString());
                    responseMap.put("status_desc","hospitaldata table does not exist!");
                }
            } else {
                responseMap.put("success", Boolean.FALSE.toString());
                responseMap.put("status_desc","team_2_database does not exist!");
            }
            responseString = gson.toJson(responseMap);


        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }

        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
	@GET
    @Path("/getpatientstatus")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllHospitalStatus() {
        String responseString = "{}";

        try {
            //in-paitent = 1, icu = 2, vent =3
			List<Map<String,String>> hospitalData = Launcher.dbEngine.getHospitalData();
            
	
			int count[] = {0,0,0};
			int vax_count[] = {0,0,0};

			for(Map<String,String> tmpHos : hospitalData){
                boolean bool_vax = Launcher.dbEngine.getVaxDataOfMrn(tmpHos.get("patient_mrn"));
				count[Integer.parseInt(tmpHos.get("patient_status"))-1]++;
				if (bool_vax){
				vax_count[Integer.parseInt(tmpHos.get("patient_status"))-1]++;
				}
            }
			Map<String,String> responseMap = new HashMap<>();
            responseMap.put("in-patient_count", Integer.toString(count[0]));
			responseMap.put("icu-patient_count", Integer.toString(count[1]));
			responseMap.put("patient_vent_count", Integer.toString(count[2]));
			if (count[0] != 0){
				Float in_p_percent = (float) vax_count[0]/ (float) count[0];
				responseMap.put("in-patient_count_vax", Float.toString(vax_count[0]/count[0]));
			}
			
			if (count[1] != 0){
				Float icu_p_percent = (float) vax_count[1]/ (float) count[1];
				responseMap.put("icu-patient_count_vax", Float.toString(vax_count[1]/count[1]));
			}
			
			if (count[2] != 0){
			Float vent_p_percent = (float) vax_count[2]/ (float) count[2];
			responseMap.put("patient_vent_count_vax", Float.toString(vax_count[2]/count[2]));
			 
			}
			
			
            

						
			responseString = gson.toJson(responseMap);
			


        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
	
	///api/getpatientstatus/{hospital_id}
    @GET
    @Path("/getpatientstatus/{hospital_id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSpecificHospitalStatus(@PathParam("hospital_id") String hid) {
        String responseString = "{}";

        /*
        in-patient_count or in-patient_vax not found in json
        icu-patient_count or icu-patient_vax not found in json
        patient_vent_count or patient_vent_vax not found in json
         */
		System.out.println("For hid: "+hid);

        try {
            //in-paitent = 1, icu = 2, vent =3

            System.out.println("For hid: "+hid);
			List<Map<String,String>> hospitalDataById = Launcher.dbEngine.getHospitalDataById(Integer.parseInt(hid));
            
            // int countAll[] = {0,0,0};
            // double[] vaxAll = {0.0, 0.0, 0.0};
            // for(Document tmpHos : hosSpecificData){
                // countAll[(Integer) tmpHos.get("patient_status")-1]++;
                // if(Launcher.vaccineMongo.getVaccinationData((String) tmpHos.get("patient_mrn"))){
                    // vaxAll[(Integer) tmpHos.get("patient_status")-1]++;
                // }
            // }

            // for(int status=0;status<3;status++){
                // res.put(statusNames[status]+constKeyCount, countAll[status]);
                // res.put(statusNames[status]+constKeyVax, ((countAll[status]>0)?vaxAll[status]/countAll[status]:0));
// //                System.out.println("Status : "+status+" Count: "+countAll[status]+" Vax: "+((countAll[status]>0)?vaxAll[status]/countAll[status]:0));
            // }
            // responseString = gson.toJson(res);
			int count[] = {0,0,0};
			int vax_count[] = {0,0,0};

			for(Map<String,String> tmpHos : hospitalDataById){
                boolean bool_vax = Launcher.dbEngine.getVaxDataOfMrn(tmpHos.get("patient_mrn"));
				count[Integer.parseInt(tmpHos.get("patient_status"))-1]++;
				if (bool_vax){
				vax_count[Integer.parseInt(tmpHos.get("patient_status"))-1]++;
				}
            }
			Map<String,String> responseMap = new HashMap<>();
			responseMap.put("hid", hid);
            responseMap.put("in-patient_count", Integer.toString(count[0]));
			responseMap.put("icu-patient_count", Integer.toString(count[1]));
			responseMap.put("patient_vent_count", Integer.toString(count[2]));
			if (count[0] != 0){
				Float in_p_percent = (float) vax_count[0]/ (float) count[0];
				responseMap.put("in-patient_count_vax", Float.toString(vax_count[0]/count[0]));
			}
			
			if (count[1] != 0){
				Float icu_p_percent = (float) vax_count[1]/ (float) count[1];
				responseMap.put("icu-patient_count_vax", Float.toString(vax_count[1]/count[1]));
			}
			
			if (count[2] != 0){
			Float vent_p_percent = (float) vax_count[2]/ (float) count[2];
			responseMap.put("patient_vent_count_vax", Float.toString(vax_count[2]/count[2]));
			 
			}
            

						
			responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
	
	@GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getResetStatus(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {
            Map<String,Object> res = new HashMap<String, Object>();
            if(reset()){
                res.put("reset_status_code", 1);
            }else{
                res.put("reset_status_code", 0);
            }

            responseString = gson.toJson(res);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
	
	@GET
    @Path("/getconfirmedcontacts/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getConfirmedContacts(
                                         @PathParam("mrn") String mrn) {
        String responseString = "{}";
        try {

            Map<String,Object> res = new HashMap<String, Object>();


        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("covid_data", "root", "rootpwd");

            res.put("contactlist", Launcher.graphDBEngine.getContacts(db,mrn));
            responseString = gson.toJson(res);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpossiblecontacts/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPossibleContacts(@PathParam("mrn") String mrn) {
        
        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("covid_data", "root", "rootpwd");
        
        String responseString = "{}";
        try {

            Map<String,Object> res = new HashMap<String, Object>();

            res.put("contactlist", Launcher.graphDBEngine.getPossibleContacts(db,mrn));
            responseString = gson.toJson(res);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
	
	public boolean reset(){
        try {

            System.out.println("Clearing Derby!");
            Launcher.dbEngine.reset("vaxdata");
			Launcher.dbEngine.reset("hospitaldata");
			System.out.println("Clearing Derby--DONE!!");
			
			System.out.println("Clearing OrientDB!");
			OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
            ODatabaseSession db = orient.open("covid_data", "root", "rootpwd");
			GraphDBEngine.clearDB(db);
			db.close();
			orient.close();
			System.out.println("Clearing OrientDB--DONE!!");
            
            return true;
        }catch (Exception ex){
            ex.printStackTrace();
            return false;
        }
    }





}
