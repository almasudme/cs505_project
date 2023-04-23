package cs505finaltemplate.Topics;

import cs505finaltemplate.graphDB.GraphDBEngine;
import cs505finaltemplate.Launcher;
import cs505finaltemplate.CEP.accessRecord;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import io.siddhi.query.api.expression.condition.In;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.Map;




public class TopicConnector {

    private Gson gson;

    final Type typeOfListMap = new TypeToken<List<Map<String,String>>>(){}.getType();
    final Type typeListTestingData = new TypeToken<List<TestingData>>(){}.getType();

    //private String EXCHANGE_NAME = "patient_data";
    Map<String,String> config;

    public TopicConnector(Map<String,String> config) {
        gson = new Gson();
        this.config = config;
    }

    public void connect() {

        try {

            //create connection factory, this can be used to create many connections
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(config.get("hostname"));
            factory.setPort(Integer.parseInt(config.get("port")));
            factory.setUsername(config.get("username"));
            factory.setPassword(config.get("password"));
            factory.setVirtualHost(config.get("virtualhost"));

            //create a connection, many channels can be created from a single connection
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            patientListChannel(channel);
            hospitalListChannel(channel);
            vaxListChannel(channel);

        } catch (Exception ex) {
            System.out.println("connect Error: " + ex.getMessage());
            ex.printStackTrace();
        }
}

    private void patientListChannel(Channel channel) {
        try {

            System.out.println("Creating patient_list channel");

            String topicName = "patient_list";
            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, topicName, "#");


            System.out.println(" [*] Paitent List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");


                List<TestingData> incomingList = gson.fromJson(message, typeListTestingData);
				
				
				// OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
				// ODatabaseSession db = orient.open("covid_data", "root", "rootpwd");
				
                for (TestingData testingData : incomingList) {

                    //Data to send to CEP
                    Map<String,String> zip_entry = new HashMap<>();
                    zip_entry.put("zip_code",String.valueOf(testingData.patient_zipcode));
                    String testInput = gson.toJson(zip_entry);
                    //uncomment for debug
                    System.out.println("testInput: " + testInput);
					// System.out.println(sizeOfincomingList);

                    //insert into CEP
                    Launcher.cepEngine.input("testInStream",testInput);

                    //do something else with each record
                    // for debug 
					
					String patient_name = testingData.patient_name;
                    String patient_mrn = testingData.patient_mrn;
					int  testing_id = testingData.testing_id;
					int patient_zipcode = testingData.patient_zipcode;
					int patient_status = testingData.patient_status;
					List<String> contact_list =   testingData.contact_list;
					List<String> event_list =   testingData.event_list;
                    // System.out.println("*Java Class*");
                    // System.out.println("\ttesting_id = " + testingData.testing_id);
                    // System.out.println("\tpatient_name = " + testingData.patient_name);
                    // System.out.println("\tpatient_mrn = " + testingData.patient_mrn);
                    // System.out.println("\tpatient_zipcode = " + testingData.patient_zipcode);
                    // System.out.println("\tpatient_status = " + testingData.patient_status);
                    // System.out.println("\tcontact_list = " + testingData.contact_list);
                    // System.out.println("\tevent_list = " + testingData.event_list);
					
                    // To Graph data
					
					GraphDBEngine.add_rec(testing_id, patient_name, patient_mrn, patient_zipcode, patient_status, contact_list, event_list);
                    
					// Prepare message for CEP
					if(testingData.patient_status == 1){
                        System.out.println("Patient : "+testingData.patient_mrn+" is +++++++. Zip code is: "+testingData.patient_zipcode);
                        //generate event based on access
                        String inputEvent = gson.toJson(new accessRecord(String.valueOf(testingData.patient_zipcode), System.currentTimeMillis()));
                        System.out.println("inputEvent: " + inputEvent);

                        //send input event to CEP
                        Launcher.cepEngine.input(Launcher.inputStreamName, inputEvent);
                    }                    
                }
			// db.close();
            // orient.close();
            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("patientListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void hospitalListChannel(Channel channel) {
        try {

            String topicName = "hospital_list";

            System.out.println("Creating hospital_list channel");

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");

            System.out.println(" [*] Hospital List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                //new message
                String message = new String(delivery.getBody(), "UTF-8");

                //convert string to class
                List<Map<String,String>> incomingList = gson.fromJson(message, typeOfListMap);
                for (Map<String,String> hospitalData : incomingList) {
                    int hospital_id = Integer.parseInt(hospitalData.get("hospital_id"));
                    String patient_name = hospitalData.get("patient_name");
                    String patient_mrn = hospitalData.get("patient_mrn");
                    int patient_status = Integer.parseInt(hospitalData.get("patient_status"));
                    //do something with each each record.
					// System.out.println("\thospital_id = " + hospital_id);
					// System.out.println("\tpatient_mrn = " + patient_mrn);
					// System.out.println("\tpatient_status = " + patient_status);
					
					//insert hospital data
					
					String insertQuery = "INSERT INTO hospitaldata VALUES (" + hospital_id + "," + "'"+patient_mrn +"'"+ "," + patient_status + ")";
					System.out.println("\tinsertQuery = " + insertQuery);
					Launcher.dbEngine.executeUpdate(insertQuery);
                }

            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("hospitalListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }


    private void vaxListChannel(Channel channel) {
        try {

            String topicName = "vax_list";

            System.out.println("Creating vax_list channel");

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");


            System.out.println(" [*] Vax List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");

                //convert string to class
                List<Map<String,String>> incomingList = gson.fromJson(message, typeOfListMap);
                for (Map<String,String> vaxData : incomingList) {
                    int vaccination_id = Integer.parseInt(vaxData.get("vaccination_id"));
                    String patient_name = vaxData.get("patient_name");
                    String patient_mrn = vaxData.get("patient_mrn");
                    //do something with each each record.
					
					//insert vaccination data
					
					String insertQuery = "INSERT INTO vaxdata VALUES (" + vaccination_id + "," + "'"+patient_mrn +"'" + ")";
					System.out.println("\tinsertQuery = " + insertQuery);
					Launcher.dbEngine.executeUpdate(insertQuery);
                }

            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("vaxListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

}
