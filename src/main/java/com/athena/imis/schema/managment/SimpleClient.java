package com.athena.imis.schema.managment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SimpleClient {

	public static void main(String[] args) {
		CostBasedSchemaManagementDOLAP20 schemaDecisionEngine = new CostBasedSchemaManagementDOLAP20(args);
		int result = schemaDecisionEngine.decideSchemaAndPopulate();

		System.out.println("PVEngine returned " + result);

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
		System.out.println("\n---------------\nSystem Catalog\n----------------\n");
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
				System.out.println(rsProps.getString(1) + "\t\t" + rsProps.getInt(2));
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
	}
}