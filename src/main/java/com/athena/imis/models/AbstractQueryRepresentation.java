/**
 * 
 */
package com.athena.imis.models;

import java.util.Set;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Represents a query abstractly, s.t., we can take it into account in schema optimization 
 * 
 * QUICK HACKS TO REPRESENT QUERIES
 * ------------------------
 * Take the query of your ppt
 * SELECT ? x ?y ?z ?w
 * WHERE { ?x worksFor ?y .
 *     ?x supervises ?z .
 *     ?z hasBirthday '2011−02−24'.
 *     ?z isMarriedTo ?w.
 *     ?w hasNationality 'GR'}
 * 
 * Query representation
 * Variables need...
 * ?x: (worksFor, supervises)
 * ?z: (hasBirthday, isMarriedTo)
 * ?w: (hasNationality)
 * ?y: *
 * Joins = {{?x, ?y}, {?x,?z}, {?z,?w}}
 * Filters = {{?z hasBirthday}, {?w, hasNationality}} 
 * 
 * 
 * Usage: 
 * ------
 * constructor; 
 * convertToPropertyIds() to translate strigns to integer id's;
 * 
 * 
 * 
 * @author pvassil
 *
 */
public class AbstractQueryRepresentation {
	//because instead of full domainspaces, queries come with aliases as prefixes, 
	//which need to be translated to the prefixes that the data have
	private Map<String, String> prefixMap;
	//for each query variables, a list of dependencies in the formal of the data (with aliases converted to data tags)
	private Map<String, List<String>> variableDependenciesAsStrings;
	//for each query variables, a list of dependencies as integers, much like the propertyMap of the schema optimizer
	private Map<String, List<Integer>> variableDependencies;
	//oti leei
	private Map<String, Set<CharacteristicSet>> candidateCSsPerVariable;
	
//	private Set<Set<String>> joins;

	public AbstractQueryRepresentation(Map<String, List<String>> pVariableDependencies,
			Map<String, String> pprefixMap) {

		if (pprefixMap != null)
			this.prefixMap = pprefixMap;
		else
			this.prefixMap 	= new HashMap<String,String>();
		
		Map<String, List<String>> prefixedVariableDependencies =  convertViaPrefixes(pVariableDependencies);
		if (prefixedVariableDependencies != null)
			this.variableDependenciesAsStrings =  prefixedVariableDependencies;
		else 
			this.variableDependenciesAsStrings = new HashMap<String, List<String>>();
		
	}//end constructor

	/*
	 * Post-constructor method that gets the job done 
	 */
	public void computeAllNeededStuffForAQR(Map<String, Integer> propertiesSet, 
			Map<CharacteristicSet, Integer> csMap) {
		convertToPropertyIds(propertiesSet);
		computeCSsPerVariable(csMap);
	}
	
	
	
	/**
	 * Populates the Map<String, List<Integer>> variableDependencies with integer values for each variable's property
	 * 
	 * @param propertiesSet a map that maps the string of a property to an id
	 * @return the size of the object's variableDependencies map
	 */
	public int convertToPropertyIds(Map<String, Integer> propertiesSet) {
		this.variableDependencies = new HashMap<String, List<Integer>>();
		for(String s: this.variableDependenciesAsStrings.keySet()) {
			ArrayList<Integer> idList =  new ArrayList<Integer>(); 
			for (String property: this.variableDependenciesAsStrings.get(s)) {
				Integer id = propertiesSet.get(property);
				if (id !=null)
					idList.add(id);
				else
					System.err.println("No mapping to id for property " + property);
			}
			this.variableDependencies.put(s, idList);
		}
		return this.variableDependencies.size();
	}//end convertToPropertyIds

	
	public int computeCSsPerVariable(Map<CharacteristicSet, Integer> csMap) {
		this.candidateCSsPerVariable = new HashMap<String, Set<CharacteristicSet>>(); 
		for(String vrbl: this.variableDependencies.keySet()) {
			Set<CharacteristicSet> vrblCSset = new HashSet<CharacteristicSet>();
			for (CharacteristicSet cs: csMap.keySet()) {
				if(cs.getAsList().containsAll(this.variableDependencies.get(vrbl)))
						vrblCSset.add(cs);
			}
			candidateCSsPerVariable.put(vrbl, vrblCSset);
		}
		return candidateCSsPerVariable.size();
	}
	
	public Set<String> getVariables() {
		return this.variableDependenciesAsStrings.keySet();
	}

	public Map<String, List<String>> getVariableDependenciesAsStrings() {
		return variableDependenciesAsStrings;
	}
	public Map<String, List<Integer>> getVariableDependencies() {
		return variableDependencies;
	}
	
	
	/**
	 * @return the candidateCSsPerVariable
	 */
	public Map<String, Set<CharacteristicSet>> getCandidateCSsPerVariable() {
		return candidateCSsPerVariable;
	}

	/**
	 * Converts the shortcut prefixes to the ontology ones.
	 * 
	 * See for example lubm queries
	 * public static String prefix = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
	 *		+ "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> ";
	 * You have to replace the rdf: and ub: query "aliases" with their respective tag,
	 * which is also the one in the data
	 * 
	 * @param pInputMap a map of Strings (typically the query variables) and their list of properties
	 * @return a transformed map where the properties of the query variables have their tag correctly replaced
	 */
	private Map<String, List<String>> convertViaPrefixes(Map<String, List<String>> pInputMap) {
		if(pInputMap == null || pInputMap.size() == 0)
			return null;
		
		Map<String, List<String>> convertedMap = new HashMap<String, List<String>>();
		for(String key: pInputMap.keySet()) {
			List<String> properties = pInputMap.get(key);
			List<String> newProperties = new ArrayList<String>();
			for(String sOld: properties) {
				String s  = sOld;
//System.out.println("s before: " + s);
				String prefix = s.substring(0,s.indexOf(":")).trim();
//System.out.println("prefix: " + prefix);

				String replacement = this.prefixMap.get(prefix).trim();
//System.out.println("repl: " + replacement);

				if (replacement != null)
					s= s.replace(prefix+":", replacement);
				s = "<" + s + ">";
//System.out.println("s after: " + s);
				newProperties.add(s);
			}
			convertedMap.put(key, newProperties);
		}

		return convertedMap;

	}//end convertViaPrefixes
	
		
	
	
}//end class




/*

public static String prefix = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
		+ "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> ";

public static String q1 = prefix + 
		"SELECT ?X WHERE"
		+ "{?X rdf:type ub:Student . "
		+ "?X ub:takesCourse <http://www.Department1.University1.edu/GraduateCourse1>}";
public static String q1_ext = prefix + 
		"SELECT ?X ?Y WHERE"
		+ "{?X rdf:type ub:GraduateStudent . "
		+ "?X ub:takesCourse ?Y . "
		+ "?Y rdf:type ?gr}"; 
*/
