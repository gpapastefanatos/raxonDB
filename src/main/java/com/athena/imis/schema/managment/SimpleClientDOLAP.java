package com.athena.imis.schema.managment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.athena.imis.models.Database;

public class SimpleClientDOLAP {

	public static void main(String[] args) {
		
		String inFile = args[1];
		Database database = new Database(args[0], args[2], args[4], args[5], Integer.parseInt(args[3]));
		int densityFactor =  Integer.parseInt(args[6]);
		
		ICostBasedSchemaManager schemaDecisionEngine = new CostBasedSchemaManagementDOLAP_Analyze(database, inFile, densityFactor);
		int result = schemaDecisionEngine.decideSchemaAndPopulate();

		System.out.println("PVEngine returned " + result);


		checkDBContents(args);
	}//end main

	
	/**
	 * Fires test queries + updates stats of all tables
	 * @param args
	 */
	private static void checkDBContents(String[] args) {
		Connection conn = null;
		try{

			Class.forName("org.postgresql.Driver");
			conn = DriverManager
					.getConnection("jdbc:postgresql://"+args[0]+":5432/" + args[2].toLowerCase(), args[4], args[5]);
			System.out.println("\n\n\nOpened CHECK database successfully");
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			System.exit(0);
		}	  
		

		try{
			Statement st = conn.createStatement();
			String propertiesSetQuery = " SELECT id, uri FROM propertiesset ORDER BY id ASC;";
			ResultSet rsProps = st.executeQuery(propertiesSetQuery);

			int size =0;
			while(rsProps.next()){
				System.out.println(rsProps.getInt(1) + "\t\t" + rsProps.getString(2));
			}
			
//			if (rsProps != null) 
//			{
//			  rsProps.last();    // moves cursor to the last row
//			  size = rsProps.getRow(); // get row id 
//			}
			
			rsProps.close();
			st.close();
		}catch(SQLException e){
			e.printStackTrace();
		}
		System.out.println("\n---------------\nSystem Catalog ESTIMATES (not exact numbers\n----------------\n");
		String analyzeQuery = "";
		try{
			Statement st = conn.createStatement();
			String propertiesSetQuery = " SELECT\r\n" + 
					"  pgClass.relname   AS tableName,\r\n" + 
					"  pgClass.reltuples AS rowCount\r\n" + 
					"FROM\r\n" + 
					"  pg_class pgClass\r\n" + 
					"LEFT JOIN\r\n" + 
					"  pg_namespace pgNamespace ON (pgNamespace.oid = pgClass.relnamespace)\r\n" + 
					"WHERE\r\n" + 
					"  pgNamespace.nspname NOT IN ('pg_catalog', 'information_schema') AND\r\n" + 
					"  pgClass.relkind='r';";
			ResultSet rsProps = st.executeQuery(propertiesSetQuery);

			int size =0;
			
			while(rsProps.next()){
				String tableName = rsProps.getString(1);
				int rowCount = rsProps.getInt(2);
				analyzeQuery = analyzeQuery + "\nANALYZE " + tableName +"; ";
				//System.out.println( tableName + "\t\t" + rowCount);
			}
			
//			if (rsProps != null) 
//			{
//			  rsProps.last();    // moves cursor to the last row
//			  size = rsProps.getRow(); // get row id 
//			}
			
			rsProps.close();
			st.close();
		}catch(SQLException e){
			e.printStackTrace();
		}
		
		System.out.println( analyzeQuery);
		Statement st;
		try {
			st = conn.createStatement();
			int res = st.executeUpdate(analyzeQuery);
			st.close();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		
		try{
			Statement stAgain = conn.createStatement();
			String propertiesSetQuery = " SELECT\r\n" + 
					"  pgClass.relname   AS tableName,\r\n" + 
					"  pgClass.reltuples AS rowCount\r\n" + 
					"FROM\r\n" + 
					"  pg_class pgClass\r\n" + 
					"LEFT JOIN\r\n" + 
					"  pg_namespace pgNamespace ON (pgNamespace.oid = pgClass.relnamespace)\r\n" + 
					"WHERE\r\n" + 
					"  pgNamespace.nspname NOT IN ('pg_catalog', 'information_schema') AND\r\n" + 
					"  pgClass.relkind='r'" +
					"ORDER BY tableName ASC;";
			ResultSet rsProps = stAgain.executeQuery(propertiesSetQuery);

			int size =0;
			
			while(rsProps.next()){
				String tableName = rsProps.getString(1);
				int rowCount = rsProps.getInt(2);
				System.out.println( tableName + "\t\t" + rowCount);
			}
			
			rsProps.close();
			stAgain.close();
		}catch(SQLException e){
			e.printStackTrace();
		}	
		
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}