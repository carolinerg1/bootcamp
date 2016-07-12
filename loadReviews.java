package com.bluefigs.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Host;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

public class loadReviews {

	
	private Cluster cluster;
	private static Session session;

	   public void connect(String node) {
		   // node is a comma delimited list of seed node IP addresses
		   // recommended: 2 seeds per Data Center
		   cluster = Cluster.builder()
	            .addContactPoint(node)	
	            .build();				
	      
	      // just for testing - make sure it connected to cluster
	      Metadata metadata = cluster.getMetadata();
	      System.out.printf("Connected to cluster: %s\n",  metadata.getClusterName());
	      
	      for ( Host host : metadata.getAllHosts() ) {
	         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
	      }
	      
	      session = cluster.connect();
	   }
	   
	   public void createSchema() { 
		   session.execute("CREATE KEYSPACE IF NOT EXISTS finefoods WITH replication " + 
				      "= {'class':'NetworkTopologyStrategy', 'Cassandra':1, 'Analytics':1, 'Solr':1};");
		   
		   System.out.println("keyspace created");
		   //session.execute("DROP TABLE IF EXISTS finefoods.review;");
		   
		   session.execute(
				      "CREATE TABLE IF NOT EXISTS finefoods.review (" +
				            "\"productId\" text," + 
				            "\"userId\" text," + 
				            "\"profileName\" text," + 
				            "\"helpfulness\" text," + 
				            "\"score\" float," +
				            "\"reviewDate\" timestamp, " +
				            "\"summary\" text," +
				            "\"review\" text," +
				            "\"reviewDateBucket\" text," +
				            "PRIMARY KEY (\"reviewDateBucket\", \"productId\", \"userId\")" +
				            ");");
			
		   System.out.println("table created");
			
	   }
	   
	   public void close() {
		   cluster.close();
	   }
	   
	   public static void main(String[] args)  {
			// get timestamp when code starts to determine how long it takes to run
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date startDate= new Date();
			
			long reviewCounter = 0; 	// used for debugging to make sure all rows were inserted
			 
			loadReviews client = new loadReviews();
		    client.connect("127.0.0.1");  	// 172.31.23.46 / 54.86.69.115 C* running on local machine -> for multi node/DC cluster, this should be comma separated list of IP
		    client.createSchema();			// only need to create schema once
		   
		    final String fileName = "/Users/caro/data/Datastax/bootcamp/finefoods_revised.txt";		//change to file location (got lazy)
		    
		    try {
		    		// readh the file
			        BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)));
					
			        String inputLine = null;
					HashMap< String,String > items = new HashMap< String,String >();
					
					String strColumns = "";
					String strValues = "";
					
					PreparedStatement statement = session.prepare(
						      "INSERT INTO finefoods.review (\"review\",\"summary\",\"reviewDate\",\"score\",\"helpfulness\",\"profileName\",\"userId\",\"productId\",\"reviewDateBucket\") " +
						      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");
					
					
					while(((inputLine = reader.readLine()) != null ) ) {
						// Split the input line into key/value based on ":"
						// Ignore empty lines.
						if(inputLine.contains(":")) {
							String[] arrLine = inputLine.split(":");
							String keyName = mapColumnName(arrLine[0].trim());
							String keyValue = arrLine[1].trim();
							
							// handle single & double quotes
							strColumns = "\"" + keyName.replace("'", "''") + "\"," + strColumns;
							strValues = "'" +  keyValue.replace("'", "''") + "'," + strValues;
							
							items.put(keyName, keyValue);
							
						}
						
						// 2 methods for inserting data: #1 - insert statement vs #2 prepared statements (faster)
						if (inputLine.equals("") && !items.isEmpty()) {
							//System.out.println("INSERT INTO finefoods.review (" + strColumns.substring(0, strColumns.length()-1) + ") " + 
							//		"VALUES (" + strValues.substring(0, strValues.length()-1) + ");");
							
							// method 1: execute
							//session.execute("INSERT INTO finefoods.review (" + strColumns.substring(0, strColumns.length()-1) + ") " + 
							//		"VALUES (" + strValues.substring(0, strValues.length()-1) + ");"
							//		);
							
							// method 2: prepared statement
							BoundStatement boundStatement = new BoundStatement(statement);
							
							// format string value read from file into correct data type
							for (Map.Entry<String, String> item : items.entrySet()) {
								if (item.getKey() == "score") {
									float floatValue = Float.parseFloat(item.getValue());
									boundStatement.setFloat(item.getKey(), floatValue);
								} else if (item.getKey() == "reviewDate") {
									int intValue = Integer.parseInt(item.getValue());
									Timestamp ts = new Timestamp(intValue);
									Date dateValue = new Date(ts.getTime());
									
									DateFormat reviewDateFormat = new SimpleDateFormat("yyyyMMddHH");
									
									boundStatement.setDate(item.getKey(), dateValue);
									boundStatement.setString("reviewDateBucket", reviewDateFormat.format(dateValue));
								} else {
									boundStatement.setString(item.getKey(), item.getValue());
								}
							}

							session.execute(boundStatement);
							
							strColumns = "";
							strValues = "";
							reviewCounter++;
						}
					}
					
					
					reader.close();
		    	  
			      
		      }
		      catch (Exception e) {
		    	  System.err.println("Error: " + e.getMessage());
		      }
		      
		      client.close();
		      
		      // get timestamp when script ended
		      Date endDate= new Date();
		      System.out.println("Start Date: " + dateFormat.format(startDate));
		      System.out.println("Rows inserted: " + reviewCounter);
		      System.out.println("End Date: " + dateFormat.format(endDate));
			    
		}
	   
	   public static boolean isNumeric(String s) {  
		    return s.matches("[-+]?\\d*\\.?\\d+");  
		}  
	   
	   public static String mapColumnName(String s) {
		   // renaming column names for readability - don't HAVE to do this
		   String newColumnName = s;
		   if (s.equals("product/productId")) {
			   newColumnName = "productId";
			   return newColumnName;
		   } else if (s.equals("review/userId")) {
			   newColumnName = "userId";
			   return newColumnName;
		   } else if (s.equals("review/profileName")) {
			   newColumnName = "profileName";
			   return newColumnName;
		   } else if (s.equals("review/helpfulness")) {
			   newColumnName = "helpfulness";
			   return newColumnName;
		   } else if (s.equals("review/score")) {
			   newColumnName = "score";
			   return newColumnName;
		   } else if (s.equals("review/time")) {
			   newColumnName = "reviewDate";
			   return newColumnName;
		   } else if (s.equals("review/summary")) {
			   newColumnName = "summary";
			   return newColumnName;
		   } else if (s.equals("review/text")) {
			   newColumnName = "review";
			   return newColumnName;
		   } else {
			   return newColumnName;
		   }
	   }
}
