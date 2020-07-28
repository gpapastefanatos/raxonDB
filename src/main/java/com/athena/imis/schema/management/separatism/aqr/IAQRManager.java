package com.athena.imis.schema.management.separatism.aqr;

import java.util.List;

import com.athena.imis.models.AbstractQueryRepresentation;

public interface IAQRManager {
	/**
	 * Create and populate a List of Abstract Query Representation objects 
	 * 
	 * @return the size of the AQR List
	 */
	int createQueries();//end createQueries()

	/**
	 * Return the List of Abstract Query Representation objects
	 * @return the queryList
	 */
	List<AbstractQueryRepresentation> getQueryList();

}