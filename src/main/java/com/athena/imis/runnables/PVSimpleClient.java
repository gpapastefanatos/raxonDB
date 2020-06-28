package com.athena.imis.runnables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PVSimpleClient {

	public static void main(String[] args) {
		PV_CostBasedRelationalLoader schemaDecisionEngine = new PV_CostBasedRelationalLoader(args);
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
			String propertiesSetQuery = " SELECT id, uri FROM propertiesset ;";
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

	}
}