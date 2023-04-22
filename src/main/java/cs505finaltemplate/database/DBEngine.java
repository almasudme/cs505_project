package cs505finaltemplate.database;


import com.google.gson.reflect.TypeToken;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DBEngine {

    private DataSource ds;
	private String databaseName = "team_2_database";
	private String derby_file = "jdbc:derby:memory:" + databaseName;

    public DBEngine() {

        try {
            //Name of database
            // String databaseName = "team_2_database";

            //Driver needs to be identified in order to load the namespace in the JVM
            String dbDriver = "org.apache.derby.jdbc.EmbeddedDriver";
            Class.forName(dbDriver).newInstance();

            //Connection string pointing to a local file location
            String dbConnectionString = derby_file + ";create=true";
            ds = setupDataSource(dbConnectionString);

            /*
            if(!databaseExist(databaseName)) {
                System.out.println("No database, creating " + databaseName);
                initDB();
            } else {
                System.out.println("Database found, removing " + databaseName);
                delete(Paths.get(databaseName).toFile());
                System.out.println("Creating " + databaseName);
                initDB();
            }
             */

            initDB();


        }

        catch (Exception ex) {
            ex.printStackTrace();
        }

    }
	public void reset(String tableName) throws IOException {
		int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                String stmtString = null;

                stmtString = "DELETE FROM " + tableName;

                Statement stmt = conn.createStatement();

                result = stmt.executeUpdate(stmtString);

                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        
    }
		
	
    public static DataSource setupDataSource(String connectURI) {
        //
        // First, we'll create a ConnectionFactory that the
        // pool will use to create Connections.
        // We'll use the DriverManagerConnectionFactory,
        // using the connect string passed in the command line
        // arguments.
        //
        ConnectionFactory connectionFactory = null;
        connectionFactory = new DriverManagerConnectionFactory(connectURI, null);


        //
        // Next we'll create the PoolableConnectionFactory, which wraps
        // the "real" Connections created by the ConnectionFactory with
        // the classes that implement the pooling functionality.
        //
        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);

        //
        // Now we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        ObjectPool<PoolableConnection> connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);

        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        PoolingDataSource<PoolableConnection> dataSource =
                new PoolingDataSource<>(connectionPool);

        return dataSource;
    }

    public void initDB() {

        String createHNode = "CREATE TABLE hospitaldata" +
                "(" +
                "   hospital_id bigint," +
                "   patient_mrn varchar(255)," +
				"   patient_status bigint"  +
                ")";

		


        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(createHNode);
					
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
		
		String createVNode = "CREATE TABLE vaxdata" +
                "(" +
                "   vaccination_id bigint," +
                "   patient_mrn varchar(255)" +
				
                ")";
				
		try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    
					stmt.executeUpdate(createVNode);
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    public int executeUpdate(String stmtString) {
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeUpdate(stmtString);
                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return  result;
    }

    public int dropTable(String tableName) {
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                String stmtString = null;

                stmtString = "DROP TABLE " + tableName;

                Statement stmt = conn.createStatement();

                result = stmt.executeUpdate(stmtString);

                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    /*
    public boolean databaseExist(String databaseName)  {
        return Paths.get(databaseName).toFile().exists();
    }
    */
    public boolean databaseExist(String databaseName)  {
        boolean exist = false;
        Connection conn;
        try {
            conn = ds.getConnection();
            if(!conn.isClosed()) {
                exist = true;
            }
            conn.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public boolean tableExist(String tableName)  {
        boolean exist = false;

        Connection conn = null;
        DatabaseMetaData metadata = null;
        ResultSet result = null;

        try {
            conn = ds.getConnection();
            metadata = conn.getMetaData();
            result = metadata.getTables(null, null, tableName.toUpperCase(), null);
            if(result.next()) {
                exist = true;
            }
            result.close();
            conn.close();

        } catch(java.sql.SQLException e) {
            e.printStackTrace();
        }

        catch(Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public List<Map<String,String>> getHospitalData() {
        List<Map<String,String>> hospitalMapList = null;
        try {

            hospitalMapList = new ArrayList<>();

            Type type = new TypeToken<Map<String, String>>(){}.getType();

            String queryString = null;

            //fill in the query
            queryString = "SELECT * FROM hospitaldata";


            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(queryString)) {

                        while (rs.next()) {
                            Map<String, String> hospitalMap = new HashMap<>();
                            hospitalMap.put("hospital_id", rs.getString("hospital_id"));
                            hospitalMap.put("patient_mrn", rs.getString("patient_mrn"));
							hospitalMap.put("patient_status", rs.getString("patient_status"));
                            hospitalMapList.add(hospitalMap);
                        }

                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return hospitalMapList;
    }
	
	
	public List<Map<String,String>> getHospitalDataById (int hid) {
		List<Map<String,String>> hospitalMapList = null;
        try {

            hospitalMapList = new ArrayList<>();

            Type type = new TypeToken<Map<String, String>>(){}.getType();

            String queryString = null;

            //fill in the query
            queryString = "SELECT * FROM hospitaldata where hospital_id = '"+hid+"';";


            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(queryString)) {

                        while (rs.next()) {
                            Map<String, String> hospitalMap = new HashMap<>();
                            hospitalMap.put("hospital_id", rs.getString("hospital_id"));
                            hospitalMap.put("patient_mrn", rs.getString("patient_mrn"));
							hospitalMap.put("patient_status", rs.getString("patient_status"));
                            hospitalMapList.add(hospitalMap);
                        }

                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return hospitalMapList;
	}
	
	
	public List<Map<String,String>> getVaxData() {
        List<Map<String,String>> vaxList = null;
        try {

            vaxList = new ArrayList<>();

            Type type = new TypeToken<Map<String, String>>(){}.getType();

            String queryString = null;

            //fill in the query
            queryString = "SELECT * FROM vaxdata";


            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(queryString)) {

                        while (rs.next()) {
                            Map<String, String> vaxMap = new HashMap<>();
                            vaxMap.put("vaccination_id", rs.getString("vaccination_id"));
                            vaxMap.put("patient_mrn", rs.getString("patient_mrn"));
							vaxList.add(vaxMap);
                        }

                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return vaxList;
}

	
	public boolean getVaxDataOfMrn(String mrn){
		boolean isVaccinated=false;
		String queryString = null;
		queryString = "SELECT vaccination_id from vaxdata WHERE patient_mrn = '"+mrn+"'";
		try(Connection conn = ds.getConnection()) {
            try (Statement stmt = conn.createStatement()) {

                try(ResultSet rs = stmt.executeQuery(queryString)) {

					if(rs.next()) {
						isVaccinated = true;
					}
					rs.close();
					conn.close();
                    }
                }
            }

        catch(Exception ex) {
            ex.printStackTrace();
        }
		return isVaccinated;
		
	}
	
	

}
