package test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.athena.imis.models.AbstractQueryRepresentation;

class AbsQueryRepresentationTester {
	private static AbstractQueryRepresentation aqr;
	private static Map<String, Integer> propertiesSet;
	
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		Map<String, String> prefixMap = new HashMap<String,String>();
		prefixMap.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		prefixMap.put("ub", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#");

		/*
		public static String q1_ext = prefix + 
				"SELECT ?X ?Y WHERE"
				+ "{?X rdf:type ub:GraduateStudent . "
				+ "?X ub:takesCourse ?Y . "
				+ "?Y rdf:type ?gr}"; 
		*/
		Map<String, List<String>> queryDependencies = new HashMap<String, List<String>>();
		queryDependencies.put("?X",Arrays.asList("rdf:type", "ub:takesCourse"));
		queryDependencies.put("?Y",Arrays.asList("rdf:type"));

		aqr = new AbstractQueryRepresentation(queryDependencies, prefixMap);
		propertiesSet = new HashMap<String, Integer>();
		propertiesSet.put("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", 0);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#takesCourse>", 1);
	}

	@Test
	void testConvertToPropertyIds() {
		for(String s: aqr.getVariableDependencies().keySet()) {
			System.out.print(s + " ->\t" );
			System.out.println(aqr.getVariableDependencies().get(s).toString());
		}
		assertEquals(2,aqr.getVariableDependencies().size());
		int conVersionSize = aqr.convertToPropertyIds(propertiesSet);
		assertEquals(2,conVersionSize);
	}

}
