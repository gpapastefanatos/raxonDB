/**
 * 
 */
package com.athena.imis.models;

import java.util.Set;

import com.athena.imis.models.ModeOfWork.WorkMode;

import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
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
	private String queryName;
	//because instead of full domainspaces, queries come with aliases as prefixes, 
	//which need to be translated to the prefixes that the data have
	private Map<String, String> prefixMap;
	//for each query variables, a list of dependencies in the formal of the data (with aliases converted to data tags)
	private Map<String, List<String>> variableDependenciesAsStrings;
	//for each query variables, a list of dependencies as integers, much like the propertyMap of the schema optimizer
	private Map<String, List<Integer>> variableDependencies;
	//oti leei
	private Map<String, Set<CharacteristicSet>> candidateCSsPerVariable;
	//a set of joins. Each join is a list of firstVariable joinProperty secondVariable
	private Set<List<String>> joinsAsStrings;
	private List<String> variablesList;
	private Set<List<CharacteristicSet>> cartesianProductOfCandidateCSs;
	private Map<CharacteristicSet, Integer> csFrequencies;

	public AbstractQueryRepresentation(String pqName,
			Map<String, List<String>> pVariableDependencies,
			Map<String, String> pprefixMap,
			Set<List<String>> pjoins) {

		this.queryName = pqName;
		if (pprefixMap != null)
			this.prefixMap = pprefixMap;
		else
			this.prefixMap 	= new HashMap<String,String>();

		Map<String, List<String>> prefixedVariableDependencies =  convertViaPrefixes(pVariableDependencies);
		if (prefixedVariableDependencies != null)
			this.variableDependenciesAsStrings =  prefixedVariableDependencies;
		else 
			this.variableDependenciesAsStrings = new HashMap<String, List<String>>();

		if (pjoins == null)
			this.joinsAsStrings = new HashSet<List<String>>();
		else
			this.joinsAsStrings = pjoins;

		this.variablesList = new ArrayList<String>(this.variableDependenciesAsStrings.keySet());
		Collections.sort(this.variablesList);

		cartesianProductOfCandidateCSs = new HashSet<List<CharacteristicSet>>();
		csFrequencies = new HashMap<CharacteristicSet, Integer>();
	}//end constructor


	/*
	 * Post-constructor method that gets the job done 
	 */
	public void computeAllNeededStuffForAQR(Map<String, Integer> propertiesSet, 
			Map<CharacteristicSet, Integer> csMap) {
		convertToPropertyIds(propertiesSet);
		fixJoinStrings();
		computeCSsPerVariable(csMap);
		computeCartesianProductOfSubqueries();
		computeCSFrequencies();
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

	/**
	 * Joins are the form ?X ub:takesCourse ?Y and the aliases must be replaced
	 * We ASSUME the input is the set<set<String>>, which is a set of join conditions, each with the format
	 *       firstVariable alias:property secondVariable
	 * if this is not the format of the string sets, this does not work. Can fix it by introducing a nested class at a later point
	 * 
	 * @return the size of the this.joins property
	 */
	public int fixJoinStrings() {
		if (this.joinsAsStrings == null || this.joinsAsStrings.size() == 0)
			return 0;
		for(List<String> joinTriplet: this.joinsAsStrings) {
			String oldString = joinTriplet.get(1);
			String newString = this.replaceStringWithPrefix(oldString);
			joinTriplet.set(1,  newString);
			System.out.println("Join: " + joinTriplet.toString() + "\n");			
		}
		return this.joinsAsStrings.size();
	}

	/**
	 * For each variable, this method computes the set of CSs that pertain to it.
	 * Ths is assuming you are given a csMap that maps each CS to an integer via the parameter csMap 
	 * 
	 * @param csMap a Map of CharacteristcSets, mapped to integeers, as computed by the schema optimizer
	 * @return the size of the Map candidateCSsPerVariable, mappings each variable to its candidate CSs
	 */
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

	/**
	 * Let VariableList be a LIST of the variables, s.t., each variable has a specific position
	 * Let C be the result of the cartsian product. We take all the candidates from all the variables
	 * The basic idea: 
	 * C = {candidateSet of the first Variable}
	 * For each variable after the first, say Xi 
	 *    CandidateNewSet Cnew
	 *    For each candidate set of Xi, say CSij,
	 *        for each element of C, say ci 
	 *           generate a new set {ci union CSij} and add it to Cnew
	 *    C = Cnew 
	 */
	public void computeCartesianProductOfSubqueries() {

		for(String vrbl: this.variablesList) {
			Set<List<CharacteristicSet>> cartesianProductNew = new HashSet<List<CharacteristicSet>>();
			if (cartesianProductOfCandidateCSs.size() == 0) {
				for (CharacteristicSet csVrlb: candidateCSsPerVariable.get(vrbl)) {
					List<CharacteristicSet> csSetCrtNew = new ArrayList<CharacteristicSet>();
					csSetCrtNew.add(csVrlb);
					cartesianProductNew.add(csSetCrtNew);
				}

			}
			else {
				for (CharacteristicSet csVrlb: candidateCSsPerVariable.get(vrbl)) {
					for (List<CharacteristicSet> csSetCrt: cartesianProductOfCandidateCSs) {
						List<CharacteristicSet> csSetCrtNew = new ArrayList<CharacteristicSet>();
						csSetCrtNew.addAll(csSetCrt);
						csSetCrtNew.add(csVrlb);
						cartesianProductNew.add(csSetCrtNew);
					}
				}
			}//end else = not the first vrbl
			cartesianProductOfCandidateCSs = cartesianProductNew;
		}//for vrbl
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {		
			System.out.println("=============================");
			for(List<CharacteristicSet> ccs: this.cartesianProductOfCandidateCSs) {
				for(CharacteristicSet cs: ccs) {
					System.out.print(cs.toString() + ",");
				}
				System.out.println("||");
			}
			System.out.println("=============================");
		}
	}// end computeCartesianProductOfSubqueries()

	public int computeCSFrequencies() {
		for(List<CharacteristicSet> nextList: this.cartesianProductOfCandidateCSs)
			for (CharacteristicSet cs: nextList) {
				if(!this.csFrequencies.containsKey(cs))
					this.csFrequencies.put(cs, 1);
				else {
					int previousCount = this.csFrequencies.get(cs);
					this.csFrequencies.replace(cs, previousCount+1);
				}
			}
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			//PV DIAGNOSTICS
			System.out.println("\n-------------CS FREQUENCIES ------------");
			for (CharacteristicSet cs: this.csFrequencies.keySet()) {
				System.out.println(cs.toString() + "\t" + this.csFrequencies.get(cs));
			}
			System.out.println("-------------------------\n");
		}
		return this.csFrequencies.size();
	}//end computeCSFrrequencies

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
				String s = replaceStringWithPrefix(sOld);
				newProperties.add(s);
			}
			convertedMap.put(key, newProperties);
		}

		return convertedMap;

	}//end convertViaPrefixes

	/**
	 * Converts a string of the form alias:property to the form <prefix#property>
	 * 
	 * @param sOld an input string with the alias, e.g., ub:Student
	 * @return a new String, with the entrie prefix in the place of the alias, as well as the enclosing <>
	 */
	private String replaceStringWithPrefix(String sOld) {
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
		return s;
	}//end replaceStringWithPrefix()




	///////////////////////////// GETTERS ////////////////
	public Set<String> getVariables() {
		return this.variableDependenciesAsStrings.keySet();
	}

	public Map<String, List<String>> getVariableDependenciesAsStrings() {
		return variableDependenciesAsStrings;
	}

	public Map<String, List<Integer>> getVariableDependencies() {
		return variableDependencies;
	}

	public Map<String, Set<CharacteristicSet>> getCandidateCSsPerVariable() {
		return candidateCSsPerVariable;
	}

	public Set<List<String>> getJoinsAsStrings() {
		return joinsAsStrings;
	}

	public Set<List<CharacteristicSet>> getCartesianProductOfCandidateCSs() {
		return cartesianProductOfCandidateCSs;
	}



	public Map<CharacteristicSet, Integer> getCsFrequencies() {
		return csFrequencies;
	}




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
