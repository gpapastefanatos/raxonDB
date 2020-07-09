package com.athena.imis.querying;

public interface IRelationalQueryArray {

	/***
	 * It translates the sparql string in an SQL query over a database of CS
	 * 
	 * @param sparql: the input SPARQL query
	 * @return the sql string
	 */

	String generateSQLQuery(String sparql);

}