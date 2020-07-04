package test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.athena.imis.models.AbstractQueryRepresentation;
import com.athena.imis.models.CharacteristicSet;

class AbsQueryRepresentationTester {
	private static AbstractQueryRepresentation aqr;
	private static Map<String, Integer> propertiesSet;
	private static Map<CharacteristicSet, Integer> csMap;
	private static Map<String, String> prefixMap;
	
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		prefixMap = new HashMap<String,String>();
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
		
		propertiesSet = new HashMap<String, Integer>();
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#advisor>" , 15);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#undergraduateDegreeFrom>" , 5);
		propertiesSet.put("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" , 0);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf>" , 4);
		propertiesSet.put("<http://www.w3.org/2002/07/owl#imports>" , 1);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#name>" , 2);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#telephone>" , 10);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#emailAddress>" , 9);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#publicationAuthor>" , 16);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#takesCourse>" , 14);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#teachingAssistantOf>" , 17);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#doctoralDegreeFrom>" , 7);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#subOrganizationOf>" , 3);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#researchInterest>" , 11);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#mastersDegreeFrom>" , 6);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#memberOf>" , 13);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#worksFor>" , 8);
		propertiesSet.put("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#headOf>" , 12);
		/*
		 * TODO: test also with sth that has ?W: * !!!!
		 */
	}

	
	/*
	public static String q1_ext = prefix + 
			"SELECT ?X ?Y WHERE"
			+ "{?X rdf:type ub:GraduateStudent . "
			+ "?X ub:takesCourse ?Y . "
			+ "?Y rdf:type ?gr}"; 
	*/

	@Test
	void testConvertToPropertyIds() {	
		Map<String, List<String>> queryDependencies = new HashMap<String, List<String>>();
		queryDependencies.put("?X",Arrays.asList("rdf:type", "ub:takesCourse"));
		queryDependencies.put("?Y",Arrays.asList("rdf:type"));

		aqr = new AbstractQueryRepresentation(queryDependencies, prefixMap, null);

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
		Map<String, List<String>> queryDependencies = new HashMap<String, List<String>>();
		queryDependencies.put("?X",Arrays.asList("rdf:type", "ub:takesCourse"));
		queryDependencies.put("?Y",Arrays.asList("rdf:type"));

		Set<List<String>> pjoins = new HashSet<List<String>>();
		pjoins.add(Arrays.asList("?X","ub:takesCourse", "?Y"));
		
		aqr = new AbstractQueryRepresentation(queryDependencies, prefixMap, pjoins);

		aqr.computeAllNeededStuffForAQR(propertiesSet,csMap);
		for(String s: aqr.getVariableDependencies().keySet()) {
			System.out.print(s + " ->\t" );
			System.out.println(aqr.getCandidateCSsPerVariable().get(s).toString());
		}
		assertEquals(2, aqr.getCandidateCSsPerVariable().size());
		assertEquals(1, aqr.getJoinsAsStrings().size());

	}

}

/*

-----------CSMAP<CS,int>: assigns an id to each cs--------------*/
