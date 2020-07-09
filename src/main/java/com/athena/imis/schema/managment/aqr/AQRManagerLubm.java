package com.athena.imis.schema.managment.aqr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


import com.athena.imis.models.AbstractQueryRepresentation;



public class AQRManagerLubm implements IAQRManager {
	private List<AbstractQueryRepresentation> queryList;
	private Map<String, String> prefixMap;

	public AQRManagerLubm() {
		queryList = new ArrayList<AbstractQueryRepresentation>();
		
		prefixMap = new HashMap<String,String>();
		prefixMap.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		prefixMap.put("ub", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#");
		
		this.createQueries();
	}

	@Override
	public int createQueries() {
		Map<String, List<String>> queryDependencies = new HashMap<String, List<String>>();
		Set<List<String>> pjoins = new HashSet<List<String>>();

		queryDependencies = new HashMap<String, List<String>>();
		pjoins = new HashSet<List<String>>();
		queryDependencies.put("?X",Arrays.asList("rdf:type", "ub:takesCourse"));
		queryList.add(new AbstractQueryRepresentation("q1",queryDependencies, prefixMap, pjoins));

		queryDependencies = new HashMap<String, List<String>>();
		pjoins = new HashSet<List<String>>();
		queryDependencies.put("?X",Arrays.asList("rdf:type", "ub:takesCourse"));
		queryDependencies.put("?Y",Arrays.asList("rdf:type"));
		pjoins.add(Arrays.asList("?X","ub:takesCourse", "?Y"));
		queryList.add(new AbstractQueryRepresentation("q1_ext",queryDependencies, prefixMap, pjoins));

		queryDependencies = new HashMap<String, List<String>>();
		pjoins = new HashSet<List<String>>();
		queryDependencies.put("?X",Arrays.asList("rdf:type", "ub:takesCourse"));
		queryDependencies.put("?Y",Arrays.asList("rdf:type"));
		queryDependencies.put("?Z",Arrays.asList("rdf:type"));
		pjoins.add(Arrays.asList("?X","ub:memberOf", "?Z"));
		pjoins.add(Arrays.asList("?Z","ub:subOrganizationOf ", "?Y"));
		pjoins.add(Arrays.asList("?X", "ub:undergraduateDegreeFrom", "?Y"));
		queryList.add(new AbstractQueryRepresentation("q2",queryDependencies, prefixMap, pjoins));

		queryDependencies = new HashMap<String, List<String>>();
		pjoins = new HashSet<List<String>>();
		queryDependencies.put("?X",Arrays.asList("rdf:type", "ub:publicationAuthor"));
		queryDependencies.put("?Y",Arrays.asList("rdf:type"));
		pjoins.add(Arrays.asList("?X","ub:publicationAuthor", "?Y"));
		queryList.add(new AbstractQueryRepresentation("q3_ext",queryDependencies, prefixMap, pjoins));

		queryDependencies = new HashMap<String, List<String>>();
		pjoins = new HashSet<List<String>>();
		queryDependencies.put("?X",Arrays.asList("rdf:type", "ub:worksFor", "ub:name", "ub:emailAddress", "ub:telephone"));
		queryDependencies.put("?Y",Arrays.asList("rdf:type"));
		pjoins.add(Arrays.asList("?X","ub:worksFor", "?Y"));
		queryList.add(new AbstractQueryRepresentation("q4_ext",queryDependencies, prefixMap, pjoins));
		
		return queryList.size();
	}//end createQueries()

	/**
	 * @return the queryList
	 */
	@Override
	public List<AbstractQueryRepresentation> getQueryList() {
		return queryList;
	}

}//endClass
