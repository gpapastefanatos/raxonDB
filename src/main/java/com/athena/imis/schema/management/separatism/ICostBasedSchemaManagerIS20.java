package com.athena.imis.schema.management.separatism;

import java.sql.Connection;

public interface ICostBasedSchemaManagerIS20 {

	/**
	 * Gets the job done FIX THIS DESCRIPTION
	 * @return 0 if all well, -1 otherwise
	 */
	int decideSchemaAndPopulate();//end decideSchemaAndPopulate()   %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

	/**
	 * Creates the database to store the triples. if its exists already, it drops it.
	 * 	
	 * @param args an array of String for the database's: servername, login, and passwd
	 * @param a Connection object to the RDBMS 
	 * @return the updated Connection to the newly created db
	 */
	Connection createDB(String args[], Connection conn);//end createDB

}