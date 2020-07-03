package test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.athena.imis.models.AbstractQueryRepresentation;
import com.athena.imis.models.CharacteristicSet;

class AbsQueryRepresentationTester {
	private static AbstractQueryRepresentation aqr;
	private static Map<String, Integer> propertiesSet;
	private static Map<CharacteristicSet, Integer> csMap;
	
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		Map<String, String> prefixMap = new HashMap<String,String>();
		prefixMap.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		prefixMap.put("ub", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#");

		csMap = new HashMap<CharacteristicSet, Integer>(); 
		csMap.put(new CharacteristicSet(Arrays.asList(0, 2, 4, 5, 6, 7, 8, 9, 10, 11)), 3 );
		csMap.put(new CharacteristicSet(Arrays.asList(0, 2, 5, 9, 10, 13, 14, 15)), 9);
		csMap.put(new CharacteristicSet(Arrays.asList(0, 2, 5, 9, 10, 13, 14, 15, 17)), 10);
		csMap.put(new CharacteristicSet(Arrays.asList(0)), 4);
		csMap.put(new CharacteristicSet(Arrays.asList(0, 2, 3)), 2 );
		csMap.put(new CharacteristicSet(Arrays.asList(0, 1)), 0 );
		csMap.put(new CharacteristicSet(Arrays.asList(0, 2)), 1 );
		csMap.put(new CharacteristicSet(Arrays.asList(0, 2, 9, 10, 13, 14)), 7);
		csMap.put(new CharacteristicSet(Arrays.asList(0, 3)), 11 );
		csMap.put(new CharacteristicSet(Arrays.asList(0, 2, 4, 5, 6, 7, 8, 9, 10)), 6 );
		csMap.put(new CharacteristicSet(Arrays.asList(0, 2, 9, 10, 13, 14, 15)), 8);
		csMap.put(new CharacteristicSet(Arrays.asList(0, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12)), 5 );
		csMap.put(new CharacteristicSet(Arrays.asList(0, 2, 16)), 12); 
		
		/*
		public static String q1_ext = prefix + 
				"SELECT ?X ?Y WHERE"
				+ "{?X rdf:type ub:GraduateStudent . "
				+ "?X ub:takesCourse ?Y . "
				+ "?Y rdf:type ?gr}"; 
		*/
		
		/*
		 * TODO: test also with sth that has ?W: * !!!!
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

		int conVersionSize = aqr.convertToPropertyIds(propertiesSet);
		assertEquals(2,conVersionSize);
		assertEquals(2,aqr.getVariableDependenciesAsStrings().size());

		for(String s: aqr.getVariableDependencies().keySet()) {
			System.out.print(s + " ->\t" );
			System.out.println(aqr.getVariableDependenciesAsStrings().get(s).toString());
			System.out.print(s + " ->\t" );
			System.out.println(aqr.getVariableDependencies().get(s).toString());
		}
	}//testConvertToPropertyIds
	
	@Test
	void testComputeAllNeededStuffForAQR() {
		aqr.computeAllNeededStuffForAQR(propertiesSet,csMap);
		for(String s: aqr.getVariableDependencies().keySet()) {
			System.out.print(s + " ->\t" );
			System.out.println(aqr.getCandidateCSsPerVariable().get(s).toString());
		}
		assertEquals(2, aqr.getCandidateCSsPerVariable().size());
	}

}

/*

-----------CSMAP<CS,int>: assigns an id to each cs--------------*/
