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
		
		queryDependencies = new HashMap<String, List<String>>();
		pjoins = new HashSet<List<String>>();
		queryDependencies.put("?X",Arrays.asList("ub:memberOf", "ub:emailAddress"));
		queryDependencies.put("?Y",Arrays.asList("rdf:type", "ub:subOrganizationOf" ));
		pjoins.add(Arrays.asList("?X","ub:memberOf", "?Y"));
		pjoins.add(Arrays.asList("?Y","ub:subOrganizationOf", "?Z"));
		queryList.add(new AbstractQueryRepresentation("q8_ext",queryDependencies, prefixMap, pjoins));

		queryDependencies = new HashMap<String, List<String>>();
		pjoins = new HashSet<List<String>>();
		queryDependencies.put("?X",Arrays.asList("rdf:type", "ub:takesCourse", "ub:name"));
		queryDependencies.put("?Y",Arrays.asList("rdf:type"));
		pjoins.add(Arrays.asList("?X","ub:takesCourse", "?Y"));
		pjoins.add(Arrays.asList("?X","ub:name", "?N"));
		queryList.add(new AbstractQueryRepresentation("q10_ext",queryDependencies, prefixMap, pjoins));

		queryDependencies = new HashMap<String, List<String>>();
		pjoins = new HashSet<List<String>>();
		queryDependencies.put("?X",Arrays.asList("rdf:type", "ub:subOrganizationOf"));
		queryDependencies.put("?Y",Arrays.asList("rdf:type"));
		pjoins.add(Arrays.asList("?X","ub:subOrganizationOf", "?Y"));
		queryList.add(new AbstractQueryRepresentation("q11_ext",queryDependencies, prefixMap, pjoins));
				
		queryDependencies = new HashMap<String, List<String>>();
		pjoins = new HashSet<List<String>>();
		queryDependencies.put("?X",Arrays.asList("ub:memberOf", "ub:headOf"));
		queryDependencies.put("?Y",Arrays.asList("rdf:type", "ub:subOrganizationOf"));
		pjoins.add(Arrays.asList("?X","ub:memberOf", "?Y"));
		pjoins.add(Arrays.asList("?Y","ub:subOrganizationOf", "?Z"));
		queryList.add(new AbstractQueryRepresentation("q12",queryDependencies, prefixMap, pjoins));
		
		queryDependencies = new HashMap<String, List<String>>();
		pjoins = new HashSet<List<String>>();
		queryDependencies.put("?s",Arrays.asList("ub:researchInterest",
				 "ub:mastersDegreeFrom",
				 "ub:doctoralDegreeFrom",
				 "ub:memberOf",
				 "rdf:type"));
		queryDependencies.put("?y",Arrays.asList("rdf:type", "ub:subOrganizationOf"));
		queryDependencies.put("?z",Arrays.asList("rdf:type"));
		pjoins.add(Arrays.asList("?s", "ub:researchInterest", "?o2"));
		pjoins.add(Arrays.asList("?s", "ub:mastersDegreeFrom", "?o3"));
		pjoins.add(Arrays.asList("?s", "ub:doctoralDegreeFrom", "?o4"));
		pjoins.add(Arrays.asList("?s", "ub:memberOf", "?y"));
		pjoins.add(Arrays.asList("?s", "rdf:type", "?o"));
		pjoins.add(Arrays.asList("?y", "rdf:type", "?o5"));
		pjoins.add(Arrays.asList("?y", "ub:subOrganizationOf", "?z"));
		pjoins.add(Arrays.asList("?z", "rdf:type", "?o6"));
		queryList.add(new AbstractQueryRepresentation("qm1",queryDependencies, prefixMap, pjoins));
		
		
		queryDependencies = new HashMap<String, List<String>>();
		pjoins = new HashSet<List<String>>();
		queryDependencies.put("?s",Arrays.asList("ub:researchInterest",
				 "ub:mastersDegreeFrom",
				 "ub:emailAddress",
				 "ub:memberOf",
				 "rdf:type"));
		queryDependencies.put("?y",Arrays.asList("rdf:type", "ub:subOrganizationOf"));
		pjoins.add(Arrays.asList("?s", "ub:researchInterest", "?o2"));
		pjoins.add(Arrays.asList("?s", "ub:mastersDegreeFrom", "?o3"));
		pjoins.add(Arrays.asList("?s", "ub:emailAddress", "?o44"));
		pjoins.add(Arrays.asList("?s", "ub:memberOf", "", "?y"));
		pjoins.add(Arrays.asList("?s", "rdf:type", "ub:UndergraduateStudent"));
		pjoins.add(Arrays.asList("?y", "rdf:type", "?o5"));
		pjoins.add(Arrays.asList("?y", "ub:subOrganizationOf", "?z"));
		queryList.add(new AbstractQueryRepresentation("qm2",queryDependencies, prefixMap, pjoins));

		
		queryDependencies = new HashMap<String, List<String>>();
		pjoins = new HashSet<List<String>>();
		queryDependencies.put("?s",Arrays.asList("ub:researchInterest",
				 "ub:mastersDegreeFrom",
				 "ub:emailAddress",
				 "ub:memberOf",
				 "rdf:type"));
		queryDependencies.put("?course",Arrays.asList("ub:name","ub:researchInterest"));
		queryDependencies.put("?student",Arrays.asList("ub:takesCourse","rdf:type","ub:memberOf"));
		queryDependencies.put("?sm",Arrays.asList("rdf:type"));
		pjoins.add(Arrays.asList("?s", "ub:researchInterest", "?o2"));
		pjoins.add(Arrays.asList("?s", "ub:mastersDegreeFrom", "?o3"));
		pjoins.add(Arrays.asList("?s", "ub:emailAddress", "?o44"));
		pjoins.add(Arrays.asList("?s", "ub:memberOf", "", "?y"));
		pjoins.add(Arrays.asList("?s", "ub:teacherOf", "?course"));
		pjoins.add(Arrays.asList("?s", "rdf:type", "?profType"));
		pjoins.add(Arrays.asList("?course", "rdf:type", "?courseType"));
		pjoins.add(Arrays.asList("?course", "ub:name", "?courseName"));
		pjoins.add(Arrays.asList("?student", "ub:takesCourse", "?course"));
		pjoins.add(Arrays.asList("?student", "rdf:type", "ub:UndergraduateStudent"));
		pjoins.add(Arrays.asList("?student", "ub:memberOf", "?sm"));
		pjoins.add(Arrays.asList("?sm", "rdf:type", "?smType"));
		queryList.add(new AbstractQueryRepresentation("qm3",queryDependencies, prefixMap, pjoins));


		queryDependencies = new HashMap<String, List<String>>();
		pjoins = new HashSet<List<String>>();
		queryDependencies.put("?s1", Arrays.asList( "rdf:type",
				 "ub:undergraduateDegreeFrom",
				 "ub:memberOf"));
		queryDependencies.put("?dept", Arrays.asList( "rdf:type",
				 "ub:subOrganizationOf"));
		queryDependencies.put("?pub", Arrays.asList( "rdf:type", "ub:publicationAuthor"));	
		pjoins.add(Arrays.asList("?s1", "rdf:type", "?studentType"));
		pjoins.add(Arrays.asList("?s1", "ub:undergraduateDegreeFrom", "?uguni"));
		pjoins.add(Arrays.asList("?s1", "ub:memberOf", "?dept"));
		pjoins.add(Arrays.asList("?dept", "rdf:type", "?deptType"));
		pjoins.add(Arrays.asList("?dept", "ub:subOrganizationOf", "?sub"));
		pjoins.add(Arrays.asList("?pub", "rdf:type", "?pubtype"));
		pjoins.add(Arrays.asList("?pub", "ub:publicationAuthor", "?s1"));
		queryList.add(new AbstractQueryRepresentation("qm4",queryDependencies, prefixMap, pjoins));


		
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
