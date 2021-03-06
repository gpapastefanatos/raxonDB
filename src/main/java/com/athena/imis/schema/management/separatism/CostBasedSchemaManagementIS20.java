package com.athena.imis.schema.management.separatism;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import com.athena.imis.models.AbstractQueryRepresentation;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

import com.athena.imis.models.CharacteristicSet;
import com.athena.imis.models.ModeOfWork;
import com.athena.imis.models.ModeOfWork.WorkMode;
import com.athena.imis.schema.management.separatism.aqr.AQRFactory;
import com.athena.imis.schema.management.separatism.aqr.IAQRManager;
//import com.athena.imis.models.DirectedGraph;
import com.athena.imis.models.Path;

import gnu.trove.map.hash.THashMap;

/**
 * An engine class, that given a file of triples, decides its relational representation and pouplates it.
 * See Meimaris et al., DOLAP 2020
 *    
 * @author meimar, gpapas, pvassil
 * @version 0.3
 */
public class CostBasedSchemaManagementIS20  implements ICostBasedSchemaManagerIS20{
	private String projectClassName;
	private Map<String, Integer> propertiesSet;
	private Map<Integer, String> revPropertiesSet;
	private Set<Integer> multiValuedProperties ;
	private Map<String, Integer> intMap ;
	private int meanMultiplier = 1;
	private String[] args;
	private String dbname;
	private int batchSize = 100;
	private Connection conn = null;
	private int[][] triplesArray = null;
	private Map<CharacteristicSet, Integer> csMap;
	private Map<Integer, Integer> dbECSMap;
	private Map<Integer, CharacteristicSet> reverseCSMap;
	private Map<Integer, int[][]> csMapFull;
	private Map<CharacteristicSet, Set<CharacteristicSet>> children;
	private Map<CharacteristicSet, Set<CharacteristicSet>> parents;
	private Map<CharacteristicSet, Set<CharacteristicSet>> allCsAncestors;
	private Map<CharacteristicSet, Integer> csExtentSizes;
	private int maxCSSize;
	private Set<CharacteristicSet> denseCSs;
	private int numDenseCSs;
	private int numDenseRows;
	private HashMap<Path, Integer> pathCosts;
	private Set<Path> foundCandidatePaths;
	private List<Path> finalListCandidatePAths;
	private Set<CharacteristicSet> finalUniqueCandidatePathsMap;
	private Iterator<Map.Entry<Integer, int[][]>> tripleGroups;
	private Map<Integer, Path> reversePathMap;
	private Map<CharacteristicSet, Path> csToPathMap;
	private Map<Path, Integer> pathMap;
	private int totalNumOfTriples;
	private HashMap <String,String> createTableStatements = new HashMap <String,String>();
	private Map<Integer, int[][]> mergedMapFull;
	private Map<CharacteristicSet, Integer> sortedCSMapByQueries;
	private List<CharacteristicSet> csToSeparate;
	private int _MaxCSKeptSeparately;
	private int _MinCSKeptSeparately;
	
	//private Logger LOG;
	//private Map<Integer, String> revIntMap;


	/**
	 * Creates the engine that decides the relational schema and populates it
	 * 
	 * @param args A String array with the following format, describe via an example: 
	 *	195.251.63.129 or localhost  -- where the pg server runs 
	 *	src/main/resources/lubm2.nt  -- where the file with the triples lies
	 *	lubm  						 -- the name of the db
	 *	100 						 -- load batch size for postgresql
	 *	postgres 					 -- db username 
	 *	postgres 					 -- db password 
	 *	2 						     -- density factor m
	 */
	public CostBasedSchemaManagementIS20(String[] args) {
		this.propertiesSet = new THashMap<String, Integer>();
		this.revPropertiesSet = new THashMap<Integer, String>();
		if(args.length > 6)
			meanMultiplier = Integer.parseInt(args[6]);
		else 
			this.meanMultiplier = 1;
		this.args = args;
		this.dbname = args[2].toLowerCase();
		this.batchSize = Integer.parseInt(args[3]);
		this.csMap = new HashMap<>();
		this.dbECSMap = null; 
		this.reverseCSMap = new HashMap<Integer, CharacteristicSet>();
		this.csMapFull = new HashMap<Integer, int[][]>();
		this.children = null;
		this.parents = new HashMap<CharacteristicSet, Set<CharacteristicSet>>();
		this.allCsAncestors = new HashMap<CharacteristicSet, Set<CharacteristicSet>>();
		this.csExtentSizes = new HashMap<CharacteristicSet, Integer>();
		this.maxCSSize = Integer.MIN_VALUE;
		this.denseCSs = new HashSet<CharacteristicSet>();
		this.numDenseCSs = 0;
		this.numDenseRows = 0;
		this.pathCosts = new HashMap<Path, Integer>();
		finalListCandidatePAths = new ArrayList<Path>();
		finalUniqueCandidatePathsMap = new HashSet<CharacteristicSet>();
		reversePathMap = new HashMap<Integer, Path>();
		csToPathMap = new HashMap<CharacteristicSet, Path>();
		this.pathMap = new HashMap<Path, Integer>();
		totalNumOfTriples=0;
		//Commented out as useless
		//revIntMap = new THashMap<Integer, String>();
		//LOG = LogManager.getLogger(CostBasedSchemaManagementIS20.class);

		Map<String,String> familyIndicatorStrings = Stream.of(new String[][] {
			  { "lubm", "LUBM" }, 
			  { "watdiv", "WATDIV" },
			  { "reactom", "REACTOME" },
			  { "geonam", "GEONAMES" },
			}).collect(Collectors.toMap(data -> data[0], data -> data[1]));
		for (String key: familyIndicatorStrings.keySet()) {
			if(args[1].trim().toLowerCase().contains(key)) {
				this.projectClassName = familyIndicatorStrings.get(key);
				break;
			}
			else
				this.projectClassName = "UNKNOWN";
		}
		System.out.println("PRJ CLASS: " + this.projectClassName);
		
		this._MinCSKeptSeparately = 1;
		this._MaxCSKeptSeparately = 5;
		

	}//end constructor


	/**
	 * Gets the job done FIX THIS DESCRIPTION
	 * @return 0 if all well, -1 otherwise
	 */
	@Override
	public int decideSchemaAndPopulate() {

		// Whenever logger is added  
		//	LOG.debug("Starting time: " + new Date().toString());
		System.out.println("Starting time: " + new Date().toString());

		this.conn = createDB(this.args, this.conn);
		if(this.conn == null) {
			System.err.println("Lost db Connection; exiting...");
			System.exit(-1);
		}

		//log start time 
		long start = System.nanoTime();
		//long end = -1;

		//load triples in triplesArray
		int numLoadedTriples = loadTriplesInSortedTriplesArray(start);
		System.out.println("Loaded triples: " + numLoadedTriples);

		//create the collections for the characteristics sets
		createCharacteristicSetCollections(start);

		//populate the this.csMapFull: for each cs, map all the triples that belong to it.
		int numCSs = extractExtentForAllCSs();
		System.out.println("csMapFull size: " + numCSs);

		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			//PV DIAGNOSTICS		
			System.out.println("\n-----------PROPERTIESSET: for each property string, an id--------------");
			for(String s: this.propertiesSet.keySet()) {
				System.out.println("propertiesSet:" + s + " -> " + propertiesSet.get(s));
			}
			System.out.println("-------------------------\n");				
			//PV DIAGNOSTICS		
			System.out.println("\n-----------REVPROPERTIESSET: for each propertyId, a string--------------");
			for(Integer i: this.revPropertiesSet.keySet()) {
				System.out.println("revPropertiesSet:" + i + " -> " + revPropertiesSet.get(i));
			}
			System.out.println("-------------------------\n");				

			//PV DIAGNOSTICS		
			System.out.println("\n-----------CSMAP<CS,int>: assigns an id to each cs--------------");
			for(CharacteristicSet cs: this.csMap.keySet()) {
				System.out.println("csMap:" + cs.toString() + " -> " + csMap.get(cs));
			}
			System.out.println("-------------------------\n");		
		
			//PV DIAGNOSTICS		
			System.out.println("\n-----------REVERSECSMAP<int,CS>: assigns a cs to each csId--------------");
			for(Integer id: this.reverseCSMap.keySet()) {
				System.out.println("revCsMap:" + id +  " -> " + reverseCSMap.get(id).toString() );
			}
			System.out.println("-------------------------\n");

			//PV DIAGNOSTICS		
			System.out.println("\n-----------DBCSMAP<int,int> size: " + dbECSMap.size() + "\n\n");

		}

		this.totalNumOfTriples = extracteAncestorAndParentRelationships();
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			System.out.println("Total number of triples: " + totalNumOfTriples);
			System.out.println("Mean size of CS extent: " + totalNumOfTriples/csMap.size()) ;
		}

		//extractDense nodes and calculate their number and the number of their triples
		this.extractDenseNodes();
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			System.out.println("Dense CSs: " + numDenseCSs);
			System.out.println("#Dense rows: " + numDenseRows);						
		}

		/**
		 * Merging paths into Dense Nodes and move merged CSs properties to the merged Dense nodes
		 * 
		 * DEN KATALABAINW TIPOTA APO TA SXOLIA!!!!! TODO:
		 * PREPEI NA DOYME TI 6A PEI PATH KAI TI/GIATI OLA AYTA!!!!
		 * 
		 * 0. Each path corresponds to only one Dense Cs and contains multiple non-dense CSs. Non dense CS may exist in multiple paths
		 * 1. It sorts paths by size (desc). Size is the sum of triples assigned to all CS in the path. 
		 * 2. It gets the biggestPath path and 
		 * 		2a. removes the COMMON cs from all other smaller paths and updates their sizes , since triples that existed in small paths are now assigned to the biggest.
		 * 3. It re-sorts the remaining paths and continues from 2. 
		 * 4. After visiting all paths, it removes empty paths + and updates all sizes
		 * 5. It processes CS not contained in any path, i.e., they are not dense and they are not parent of dense.
		 * 		5a. It redefines dense nodes threshold in the remaining CS, as  Math.max((notCovered)/notContained.size()*meanMultiplier, total/1000)){
		 * 		5b. It finds new paths from remaining CSs, given new dense nodes.  
		 * 		5c. It continues from 2-4 for remaining paths
		 * 6. It stores everything left until 5 in a big CS.  
		 *    
		 */

		//steps [1 .. 4]
		List<Path> orderedPaths = this.extractCandidatePathsSortedOnTripleNumber();
		this.removeNestedAndEmptyPaths(orderedPaths);


		//steps 5 and 6, before we refactor this mega-method, must explain to me. 		
		createCSMergersInPaths();

		//compute which CSs to isolate
		this.extracteCSToIsolate();		

		//create separate paths for them
		this.extractSeparatistCSToSeparatePaths();

		// for each path (i.e., table to be created), compute its triples via the map mergedMapFull 
		assignTuplesToTheirPath();
		
		/* *************************************************************************************************************		
		 *   Finally, populate the db with the triples stored as tuples	
		 * ************************************************************************************************************ */
		int dbPopulationResult = createTablesPopulateDatabase(dbECSMap, reverseCSMap, pathMap, csToPathMap, reversePathMap, tripleGroups);
		System.out.println("Ending time: " + new Date().toString());

		if(ModeOfWork.mode != WorkMode.EXPERIMENT) {		
			//PV DIAGNOSTICS		
			System.out.println("\n-----------CREATE TABLE--------------");
			for(String stmt: this.createTableStatements.keySet() ) {
				System.out.println("CREATE TABLE: " + stmt + " -> " + this.createTableStatements.get(stmt));
			}	
		}
		return 0;
	}//end decideSchemaAndPopulate()   %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



	// /////////////////////////////////////////////////////////////////////////////////////
	// ///////////////////////////////////////////////////////////////////////////////////
	// ///////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * For each path (i.e., table to be created), compute its triples via the map mergedMapFull
	 */
	private int assignTuplesToTheirPath() {
		mergedMapFull = new HashMap<Integer, int[][]>();
		
		for(Path pathP : pathMap.keySet()) {
			int[][] concat = csMapFull.get(csMap.get(pathP.get(0)));
			for(int i = 0; i < pathP.size(); i++){
				concat = ArrayUtils.addAll(concat, csMapFull.get(csMap.get(pathP.get(i)))) ;
			}
			int pathId = pathMap.get(pathP);
			mergedMapFull.put(pathId, concat);
		}
		tripleGroups = mergedMapFull.entrySet().iterator();
		
		return mergedMapFull.size();
	}


	
	/**
	 * Give the set of CSs to be separated and the csToPathMap which has already been populated, removes the separatist CSs from their previous paths
	 * and puts them into new, isolated paths
	 */
	private int extractSeparatistCSToSeparatePaths() {
		System.out.println("\nComputing SEPARATISTS: ");
		for (CharacteristicSet cs: csToSeparate) {
			Path path = csToPathMap.get(cs);
			int pathSize = path.size();
			//if it is already on its own
			if (pathSize == 1) {
			System.out.println("Omit path: " + path.toString());
				continue;
			}
			//if merged with others
			Path soloPath = new Path();
			soloPath.add(cs);
			System.out.println("\t" + soloPath.size() + " New path: " + soloPath.toString());

			//Replace the path of the cs in the collective path maps with a new
			csToPathMap.replace(cs, soloPath);
			int newPathId = this.pathMap.size();
			this.pathMap.put(soloPath, newPathId);
			this.reversePathMap.put(newPathId, soloPath);
			
			path.remove(cs);
			//this.pathMap.put(path, oldPathId);
		}
		
		this.pathMap = new HashMap<Path,Integer>();
		for(Integer id: reversePathMap.keySet()) {
			Path path = reversePathMap.get(id);
			this.pathMap.put(path, id);
		}
		
		//PV DIAGNOSTICS
		if(ModeOfWork.mode != WorkMode.EXPERIMENT) {
			System.out.println("\n-----------AGAIN PATHMAP: an id for each path--------------");
			for(Path path: pathMap.keySet()) {
				System.out.println("PathMap:" + path.toString() + " -> " + pathMap.get(path));
			}
			System.out.println("-------------------------\n");	
			System.out.println("\n-----------AGAIN REVPATHMAP: an id for each path--------------");
			for(Integer id: this.reversePathMap.keySet() ) {
				System.out.println("RPathMap: " + id + " -> " + reversePathMap.get(id).toString());
			}
		}
		System.out.println("\nDone with Computing SEPARATISTS. ");
		return pathMap.size();
	}// endextractSeparatistCSToSeparatePaths()


	/**
	 * Computes which CS's to isolate  as separate tables, in the collection csToSeparate
	 */
	private int  extracteCSToIsolate() {
		IAQRManager aqrMgr = AQRFactory.createAQRManager(this.projectClassName); 
				//new AQRManagerLubm();
		List<AbstractQueryRepresentation> queryList = aqrMgr.getQueryList();
		Set<AbstractQueryRepresentation> querySet = queryList.stream().collect(Collectors.toSet());
		this.sortedCSMapByQueries = this.computeFrequencies(querySet);

		int howManyToSeparate = whichCSToKeepUntouchedSimple(this.sortedCSMapByQueries, _MinCSKeptSeparately, _MaxCSKeptSeparately);
		System.out.println("\nTO KEEP SEPARATELY: " + howManyToSeparate);

		this.csToSeparate = new ArrayList<CharacteristicSet>(); 
		return extractCSToIsolate(sortedCSMapByQueries, howManyToSeparate, this.csToSeparate);
	}


	/**
	 * Copy the appropriate CS's to a separate list
	 * 
	 * @param sortedCSMapByQueries the map of CS and their frequencies in the query workload
	 * @param howManyToSeparate the number of CS's to separate, as already calculated
	 * @param csToSeparate the list of CS's to populate
	 */
	private int extractCSToIsolate(Map<CharacteristicSet, Integer> sortedCSMapByQueries, int howManyToSeparate,
			List<CharacteristicSet> csToSeparate) {
		Iterator<CharacteristicSet> iter = sortedCSMapByQueries.keySet().iterator();
		int i = 0;
		while(iter.hasNext() && i< howManyToSeparate) {
			CharacteristicSet cs = iter.next(); 
			csToSeparate.add(cs);
			i++;
			if(ModeOfWork.mode != WorkMode.EXPERIMENT) {
				System.out.println("Separated:\t" + cs.toString());
			}
		}
		if(ModeOfWork.mode != WorkMode.EXPERIMENT) {
			System.out.println();
		}
		return csToSeparate.size();
	}



	/**
	 * Returns a sorted map of frequencies for each characteristic set of a workflow.
	 * 
	 * @param querySet
	 * @return
	 */
	private Map<CharacteristicSet, Integer> computeFrequencies(Set<AbstractQueryRepresentation> querySet) {
		//Set<CharacteristicSet> result = new HashSet<CharacteristicSet>();
		Map<CharacteristicSet, Integer> queryFrequencies = new HashMap<CharacteristicSet, Integer>();

		for (AbstractQueryRepresentation aqr: querySet) {
			aqr.computeAllNeededStuffForAQR(this.propertiesSet,this.csMap); 
			Map<CharacteristicSet, Integer> localQueryFrequencies = aqr.getCsFrequencies();

			for(CharacteristicSet cs: localQueryFrequencies.keySet()) {
				int localCounter = localQueryFrequencies.get(cs);
				if(!queryFrequencies.containsKey(cs))
					queryFrequencies.put(cs,localCounter);
				else {
					int counterOld = queryFrequencies.get(cs); 
					queryFrequencies.replace(cs, counterOld+localCounter);
				}
			}
		}//for each query

		//now sort the map desc by value
		Map<CharacteristicSet, Integer> queryFrequenciesSorted = queryFrequencies
				.entrySet()
				.stream()
				.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
						LinkedHashMap::new));
		//if(ModeOfWork.mode != WorkMode.EXPERIMENT) {
			System.out.println("\n\n&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& Sorted Map: + &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
			System.out.println( Arrays.toString(queryFrequenciesSorted.entrySet().toArray()));
			System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n\n");	
		//}
		return queryFrequenciesSorted;
	}//end highlightPopularCSs


	/**
	 * We have a sorted list of frequencies. We want to keep separately a set of them that make a difference.
	 * We continue keeping as long as the difference rate is not dropping.
	 * We also give as a parameter (to allow exp's) for a min and a max number of CS's we gonna keep separate.
	 * We return the computed value if it falls between min and max; otherwise the appropriate of the two
	 * (min if the computed value is smaller than min, max if it's larger than max).
	 * 
	 * @param sortedQueryFrequenciesDesc the map of CSs which is sorted desc by frequency values
	 * @param minNumberOfCSs the minimum number of CS that we will obligatorily separate
	 * @param maxNumberOfCSs the max number of CS that we will tolerate to see separated
	 * @return how many CS's to separate
	 */
	private int whichCSToKeepUntouchedSimple(Map<CharacteristicSet, Integer> sortedQueryFrequenciesDesc, int minNumberOfCSs, int maxNumberOfCS) {

		ArrayList<Integer> sortedValues =  new ArrayList<Integer>(sortedQueryFrequenciesDesc.values());
		int position = 0;
		int currentValue; int nextValue;
		double stoppingDropPct = Double.MIN_VALUE;
		while (position < sortedValues.size()-1) {
			currentValue = sortedValues.get(position);
			nextValue = sortedValues.get(position+1);
			int delta = currentValue - nextValue;
			if(delta == 0) {
				position++;
			}
			else {
				double dropPct = (delta) / ((double)currentValue);
				if (dropPct >= stoppingDropPct) {   //we are dropping at least as fast as before, continue
					stoppingDropPct = dropPct;
					position++;
				}
				else { //we stopped dropping, stop
					break;
				}//else: we are dropping as a fraction
			}//else: not equal to previous value
		}//end while

		if (position >= maxNumberOfCS)
			return maxNumberOfCS;
		else
			return Math.max(minNumberOfCSs, position);
		

	}//end whichCSToKeepUntouched()




	/**
	 * TODO REFACTOR ME REFACTOR ME REFACTOR ME
	 */
	private void createCSMergersInPaths() {
		/**
		 * Process CS not contained or covered in paths  
		 */
		int notCovered = 0;
		Set<CharacteristicSet> notContained = new HashSet<CharacteristicSet>();
		for(CharacteristicSet nextCS : csMap.keySet()){
			if(!finalUniqueCandidatePathsMap.contains(nextCS)){
				if(ModeOfWork.mode != WorkMode.EXPERIMENT) {
					//PV DIAGNOSTICS
					System.out.println("Id of CS not contained: " + nextCS.toString()) ;
				}
				notCovered += csExtentSizes.get(nextCS);
				notContained.add(nextCS);
			}
		}
		System.out.println("#Tuples not covered : " + notCovered); 

		Map<Path, Integer> remainingCosts = new HashMap<Path, Integer>();
		Map<CharacteristicSet, Set<CharacteristicSet>> remainingAncestors = new HashMap<CharacteristicSet, Set<CharacteristicSet>>();
		//				MinHash minhash = new MinHash(3, propertiesSet.size());
		//				Map<NewCS, int[]> signatures = new HashMap<NewCS, int[]>();
		//				for(NewCS parent : notContained){
		//					signatures.put(parent, minhash.signature(new HashSet<Integer>(parent.getAsList())));
		//				}
		for(CharacteristicSet parent : notContained){

			for(CharacteristicSet child: notContained){

				if(parent.equals(child)) continue;

				if(child.contains(parent)){
					Set<CharacteristicSet> children = remainingAncestors.getOrDefault(parent, new HashSet<CharacteristicSet>());
					children.add(child);
					remainingAncestors.put(parent, children);
				}
				else{							
					//							double sim = minhash.similarity(signatures.get(child), signatures.get(parent));
					//							if(sim>0.9){
					//								Set<NewCS> children = remainingAncestors.getOrDefault(parent, new HashSet<NewCS>());
					//								children.add(child);
					//								remainingAncestors.put(parent, children);
					//							}
					if(jaccardSimilarity(child.getAsList(), parent.getAsList())>0.9){
						Set<CharacteristicSet> children = allCsAncestors.getOrDefault(parent, new HashSet<CharacteristicSet>());
						children.add(child);
						remainingAncestors.put(parent, children);
					}
				}

			}

		}
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			System.out.println("Remaining ancestor listing complete.\n\n");
		}



		Map<CharacteristicSet, Set<CharacteristicSet>> remainingImmediateAncestors = getChildren(remainingAncestors);
		Map<CharacteristicSet, Set<CharacteristicSet>> reverseImmediateAncestorsNotContained = new HashMap<CharacteristicSet, Set<CharacteristicSet>>();
		for(CharacteristicSet f : notContained){
			reverseImmediateAncestorsNotContained.put(f, new HashSet<CharacteristicSet>());
		}
		for(CharacteristicSet nextCS : remainingImmediateAncestors.keySet()){
			for(CharacteristicSet child : remainingImmediateAncestors.get(nextCS)){
				Set<CharacteristicSet> set = reverseImmediateAncestorsNotContained.getOrDefault(child, new HashSet<CharacteristicSet>());
				set.add(nextCS);
				reverseImmediateAncestorsNotContained.put(child, set);
			}
		}
		//Now for the remaining paths.
		Set<CharacteristicSet> remainingDenseCSs = new HashSet<CharacteristicSet>();
		for(CharacteristicSet nextCS : notContained) {
			//if(children.containsKey(nextCS))
			//	continue;


			/*
			 * DECIDE IF DENSE !!!!! MUST ALIGN TO THE OTHER DEF.
			 */

			if(csExtentSizes.get(nextCS) >= Math.max((notCovered)/notContained.size()*meanMultiplier, totalNumOfTriples/1000)){
				//denseCS++;
				remainingDenseCSs.add(nextCS);
				//totalDenseRows += csSizes.get(nextCS);
			}
		}
		Set<Path> remainingPaths = findPaths(remainingDenseCSs, remainingCosts, csExtentSizes, reverseImmediateAncestorsNotContained, true, true);
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			//PV DIAGNOSTICS
			System.out.println("\n---------REM. DENSE----------------");		
			for(CharacteristicSet cs: remainingDenseCSs) {
				System.out.println("Remaining Dense:" + cs.toString());
			}
			System.out.println("\n-----------REM. PATH--------------");
			for(Path path: remainingPaths) {
				System.out.println("Remaining Path:" + path.toString());
			}
			System.out.println("-------------------------\n");
		}

		int totalCovered = 0;

		for(Path nextPath : finalListCandidatePAths){
			//System.out.println("next path: " + nextPath );
			totalCovered += pathCosts.get(nextPath);

		}
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			System.out.println("Total coverage: " + totalCovered) ;
		}

		int totalRemaining = 0;

		for(Path nextPath : remainingPaths){
			//System.out.println("next path: " + nextPath );
			totalRemaining  += remainingCosts.get(nextPath);

		}
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			System.out.println("Total remaining:" + totalRemaining) ;
			System.out.println("Size of remaining Paths: " + remainingPaths.size());
		}
		//		start remaining cleanup
		for(Path outerPath : remainingPaths){
			for(Path innerPath : remainingPaths){
				if(outerPath.equals(innerPath)) continue;					
				if(innerPath.containsAll(outerPath)){
					remainingPaths.remove(outerPath);
					break;
				}					
			}
		}	
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			System.out.println("Removed contained.");

			//				for(Path nextPath : foundPaths){
			//					System.out.println(pathCosts.get(nextPath)+": " + nextPath.toString()) ;
			//				}
			//System.out.println("\n\n\n\n");

			System.out.println("Sorting...");
		}
		List<Path> remainingOrderedPaths = new ArrayList<Path>(remainingPaths);			
		Collections.sort(remainingOrderedPaths, new Comparator<Path>() {

			public int compare(Path o1, Path o2) {
				if (remainingCosts.get(o1) > remainingCosts.get(o2)) return -11;
				else if (remainingCosts.get(o1) < remainingCosts.get(o2)) return 1;
				else return 0;

			}
		});
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			System.out.println("Done.");
			//				for(Path nextPath : orderedPaths){
			//					System.out.println(pathCosts.get(nextPath)+": " + nextPath.toString()) ;
			//				}
		}
		List<Path> remainingFinalList = new ArrayList<Path>();

		//int totalIterations = 0;					
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			System.out.println("Pruning...");
		}

		Path bigPath;

		for(int i = 0; i < remainingOrderedPaths.size(); i++){

			bigPath = remainingOrderedPaths.get(i);

			for(int k = i+1; k < remainingOrderedPaths.size(); k++){

				Path nextCS = remainingOrderedPaths.get(k);

				nextCS.removeAll(bigPath);

				updateCardinality(nextCS, csExtentSizes, remainingCosts);	

			}				

			Collections.sort(remainingOrderedPaths.subList(i+1, remainingOrderedPaths.size()), new Comparator<Path>() {

				public int compare(Path o1, Path o2) {
					if (remainingCosts.get(o1) > remainingCosts.get(o2)) return -1;
					else if (remainingCosts.get(o1) < remainingCosts.get(o2)) return 1;
					else return 0;

				}
			});

		}
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			System.out.println("Done.");
		}

		Iterator<Path> finalIt;
		finalIt = remainingOrderedPaths.iterator();
		while(finalIt.hasNext()){
			Path n = finalIt.next();
			if(n.isEmpty()){
				finalIt.remove();
				remainingCosts.remove(n);
			}
			else{
				updateCardinality(n, csExtentSizes, remainingCosts);
				remainingFinalList.add(n);
			}
		}
		Set<CharacteristicSet> remainingFinalUnique = new HashSet<CharacteristicSet>();
		for(Path finalCS : remainingFinalList){

			remainingFinalUnique.addAll(finalCS);
		}

		//				for(Path nextPath : remainingFinalList){
		//					System.out.println(remainingCosts.get(nextPath)+": " + nextPath.toString()) ;
		//				}
		totalRemaining = 0;
		for(Path nextPath : remainingFinalList){

			//System.out.println("next path: " + nextPath );
			totalRemaining  += remainingCosts.get(nextPath);

		}
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			System.out.println("Total remaining:" + totalRemaining) ;
			System.out.println("Remaining Unique: " + remainingFinalUnique.size());
		}
		//end remaining cleanup





		int coveredSoFar = totalCovered + totalRemaining;
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			System.out.println("Dataset coverage: "  +  ((double)coveredSoFar/(double)totalNumOfTriples));
		}
		

		
		finalListCandidatePAths.addAll(remainingFinalList);

		Set<CharacteristicSet> finalNotCovered = new HashSet<CharacteristicSet>();
		Path finalNotCoveredPath = new Path();

		for(CharacteristicSet abandoned : csMap.keySet()){
			if(finalUniqueCandidatePathsMap.contains(abandoned) || remainingFinalUnique.contains(abandoned)){
				continue;
			}
			else{
				finalNotCovered.add(abandoned);
				finalNotCoveredPath.add(abandoned);
			}
		}
		
		//also compute new density factor
		int pathIndex = 0;
		mergedMapFull = new HashMap<Integer, int[][]>();
		int totalCSInPaths = 0;
		for(Path pathP : finalListCandidatePAths){
			pathMap.put(pathP, pathIndex++);
			int[][] concat= csMapFull.get(csMap.get(pathP.get(0)));
			//			int[][] concat = triples;
			csToPathMap.put(pathP.get(0), pathP);
			totalCSInPaths += pathP.size();
			for(int i = 1; i < pathP.size(); i++){
				concat = ArrayUtils.addAll(concat, csMapFull.get(csMap.get(pathP.get(i)))) ;
				csToPathMap.put(pathP.get(i), pathP);						
			}
			int pathId = pathMap.get(pathP);
//OEO
			//			mergedMapFull.put(pathId, concat);

		}

		//now for the abandoned
		if (finalNotCoveredPath.size()>0) {
			pathMap.put(finalNotCoveredPath, pathIndex++);
			int[][] triples = csMapFull.get(csMap.get(finalNotCoveredPath.get(0)));
			int[][] concat = triples;
			csToPathMap.put(finalNotCoveredPath.get(0), finalNotCoveredPath);				
			for(int i = 1; i < finalNotCoveredPath.size(); i++){
				concat = ArrayUtils.addAll(concat, csMapFull.get(csMap.get(finalNotCoveredPath.get(i)))) ;
				csToPathMap.put(finalNotCoveredPath.get(i), finalNotCoveredPath);						
			}
//OEO
			//mergedMapFull.put(pathMap.get(finalNotCoveredPath), concat);
			
		}

		for(Integer pathId: mergedMapFull.keySet()) {
			System.out.println("&&& " + pathId); //+ " &&& " + this.reversePathMap.get(pathId).toString());
		}
	
		double density = (double) coveredSoFar / totalCSInPaths ;  
		System.out.println("Density: " + density);
		for(Path pathP : pathMap.keySet())
			reversePathMap.put(pathMap.get(pathP), pathP);
		//if(true) return ;
		System.out.println("merged map full: " + mergedMapFull.toString());
		
//OEO
		//tripleGroups = mergedMapFull.entrySet().iterator();

		if(ModeOfWork.mode != WorkMode.EXPERIMENT) {
			//PV DIAGNOSTICS
			System.out.println("\n-----------CSTOPATHMAP: for each cs, in which path it will go--------------");
			for(CharacteristicSet cs: csToPathMap.keySet()) {
				System.out.println("csToPathMap:" + cs.toString() + " -> " + csToPathMap.get(cs).toString());
			}
			System.out.println("-------------------------\n");
			//PV DIAGNOSTICS		
			System.out.println("\n-----------PATHMAP: an id for each path--------------");
			for(Path path: pathMap.keySet()) {
				System.out.println("PathMap:" + path.toString() + " -> " + pathMap.get(path));
			}
			System.out.println("-------------------------\n");
			//PV DIAGNOSTICS		
			System.out.println("\n-----------REVPATHMAP: an id for each path--------------");
			for(Integer id: this.reversePathMap.keySet() ) {
				System.out.println("PathMap: " + id + " -> " + reversePathMap.get(id).toString());
			}
			System.out.println("-------------------------\n");		
			//PV DIAGNOSTICS
			System.out.println("\n-----------MERGEDMAP: tuples per path --------------");
			for(Integer id: mergedMapFull.keySet()) {
				System.out.println("MergedMap:" + id + " -> " + mergedMapFull.get(id).length);
			}
			System.out.println("-------------------------\n");

		}
	}//end cannotRefactorStep5UnlessYouExplainItToMe()










	/**
	 * Creates the database to store the triples. if its exists already, it drops it.
	 * 	
	 * @param args an array of String for the database's: servername, login, and passwd
	 * @param a Connection object to the RDBMS 
	 * @return the updated Connection to the newly created db
	 */
	@Override
	public Connection createDB(String args[], Connection conn) {
		try {
			Class.forName("org.postgresql.Driver");

			conn = DriverManager
					.getConnection("jdbc:postgresql://"+args[0]+":5432/", args[4], args[5]);

			Statement cre = conn.createStatement();
			cre.executeUpdate("DROP DATABASE IF EXISTS "+ this.dbname +" ;");	         
			System.out.println("Droped database successfully");

			int resDBC = cre.executeUpdate("CREATE DATABASE "+ this.dbname +" ;COMMIT;");
			System.out.println("Database creation returned " + resDBC);
			cre.close();
			conn.close();

			conn = DriverManager
					.getConnection("jdbc:postgresql://"+args[0]+":5432/" + this.dbname, args[4], args[5]);			         			        				

			System.out.println("Opened database successfully");

		} catch ( Exception e ) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			System.exit(0);
		}	      

		return conn;

	}//end createDB


	/**
	 * Reads a file and stores the triples in an array.
	 * 
	 * Reads the Input .nt File and stores triples in an array, spec., this.triplesArray[#triple][4] --> [s, p, o, csId] 
	 * 
	 * In other words, for each triple i, tripleArrays[i] is the four-integer array with the s,p,o,csId of the triple
	 * At the end of the method, the this.triplesArray is sorted by subject
	 * 
	 * An auto-increment is assigned to {s, o} (s,o share the same index) and p based on the order of occurrence
	 * csID is initialized to -1 (it is filled later when CS are constructed)
	 * 
	 * @param start A long value to denote the start of time measurement and contrast it to individual time measurements 
	 * for the different steps within the method
	 * @return returns the number of triples stored in the triplesArray
	 */
	private int loadTriplesInSortedTriplesArray(long start) {
		long end;

		// no of triples in file
		int triplesParsed2 = 0;
		// index for array of triples 
		int next = 0; 
		//index for no of properties in triples
		int propIndex = 0;
		//index for no of nodes, i.e., (s) or (o) in triples.
		int  nodeIndex = 0;  

		FileInputStream is;
		try {
			is = new FileInputStream(args[1]);
			NxParser nxp = new NxParser();
			nxp.parse(is);
			for (@SuppressWarnings("unused") Node[] nx : nxp){
				triplesParsed2++;
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} 	

		System.out.println("triplesParsed: " + triplesParsed2);
		System.out.println(System.nanoTime()-start);

		this.triplesArray = new int[triplesParsed2][4];

		intMap = new THashMap<String, Integer>(triplesParsed2);
		String s, p, o;

		try {
			int[] ar ;
			is = new FileInputStream(args[1]);
			NxParser nxp = new NxParser();

			nxp.parse(is);
			for (Node[] nx : nxp){
				if(triplesParsed2==0) break;
				triplesParsed2--;
				s = nx[0].toString();
				p = nx[1].toString();
				o = nx[2].toString();
				if(!propertiesSet.containsKey(p)){							
					revPropertiesSet.put(propIndex, p);
					propertiesSet.put(p, propIndex++);	    		
				}

				if(!intMap.containsKey(s)){		    				    
					//revIntMap.put(nextInd, s);
					intMap.put(s, nodeIndex++);
				}

				if(!intMap.containsKey(o)){		   			   
					//if(triple.getObject().isURI())
					//revIntMap.put(nextInd, o);
					intMap.put(o, nodeIndex++);
					//else
					//intMap.put(o, Integer.MAX_VALUE);
				}

				ar = new int[4];
				ar[0] = intMap.get(s);//spLong;
				ar[1] = propertiesSet.get(p);//spLong;
				ar[2] = intMap.get(o);//spLong;			
				ar[3] = -1;

				//Populate the triplesArray with the data for the next Triple
				triplesArray[next++] = ar;
				//next++;
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		end = System.nanoTime();

		System.out.println("piped: " + (end-start));

		/*
		 * Sort the array of triples in ascending order of subject node.   
		 */
		Arrays.sort(triplesArray, new Comparator<int[]>() {
			public int compare(int[] s1, int[] s2) {
				if (s1[0] > s2[0])
					return 1;    // tells Arrays.sort() that s1 comes after s2
				else if (s1[0] < s2[0])
					return -1;   // tells Arrays.sort() that s1 comes before s2
				else {
					return 0;
				}
			}
		});
		end = System.nanoTime();
		System.out.println("sorting: " + (end-start));

		return triplesArray.length;
	}//end loadTriplesInSortedTriplesArray



	/**
	 * A method that iterate over the triples,and creates Characteristics CSs which are stored in 3 HashMaps
	 * CSMap<CharacteristicSet,Integer> : assigns an index (e.g., csID) to each cs; a CS is a list of properties {p1,p2,...,pn} (int representation)
	 * reverseCSMap<Integer,CharacteristicSet>: keeps the reverse mapping from index (csID) to a CS  
	 * dbECSMap: keeps a mapping between a subject index and a csID
	 * @param start a long value to help with time measurements
	 */
	private void createCharacteristicSetCollections(long start) {
		long end;
		/**

		 */
		this.dbECSMap = new THashMap<Integer, Integer>(triplesArray.length/10);
		int previousSubject = Integer.MIN_VALUE;


		//gp addition for graph-based management of CS
		//		DirectedGraph<CharacteristicSet> csGraph = new DirectedGraph<CharacteristicSet>();
		//		ArrayList<int[]> csTriples;


		int csIndex = 0;
		/*for(int i = 0; i < l.size(); i++){
					long t = l.apply((long)i);*/
		int previousStart = 0;
		CharacteristicSet cs = null;


		int subject ;
		int prop ;

		List<Integer> propList = new ArrayList<Integer>();;

		//array is sorted on s
		for(int i = 0; i < triplesArray.length; i++){
			subject = triplesArray[i][0];
			prop = triplesArray[i][1];

			// if the subject has changed check if a new CS is needed 
			if(i > 0 && previousSubject != subject){

				cs = new CharacteristicSet(propList);	

				if(!csMap.containsKey(cs)){
					//					csTriples = new ArrayList<int[]>();
					dbECSMap.put(previousSubject, csIndex);
					reverseCSMap.put(csIndex, cs);
					//update with the new csID the triples array for all triples mapped to this CS
					for(int j = previousStart; j < i; j++) {
						triplesArray[j][3] = csIndex;
						//						csTriples.add(array[j]);
					}
					csMap.put(cs, csIndex++);
					//					cs.setTriples(csTriples);
					//					csGraph.addNode(cs);
				}
				else{
					dbECSMap.put(previousSubject, csMap.get(cs));
					//					csTriples = cs.getTriples();
					//array[i-1][3] = ucs.get(cs);
					for(int j = previousStart; j < i; j++) {
						triplesArray[j][3] = csMap.get(cs);
						//						csTriples.add(array[j]);
					}
					//					cs.setTriples(csTriples);

				}
				previousStart = i;
				propList = new ArrayList<Integer>();
			}
			if(!propList.contains(prop))
				propList.add(prop);
			previousSubject = subject;
		}


		if(!propList.isEmpty()){
			cs = new CharacteristicSet(propList);
			if(!csMap.containsKey(cs)){
				//				csTriples = new ArrayList<int[]>();
				//array[array.length-1][3] = csIndex; 
				for(int j = previousStart; j < triplesArray.length; j++) {
					triplesArray[j][3] = csIndex;
					//					csTriples.add(array[j]);
				}
				dbECSMap.put(previousSubject, csIndex);
				reverseCSMap.put(csIndex, cs);
				csMap.put(cs, csIndex);
				//				cs.setTriples(csTriples);
				//				csGraph.addNode(cs);
			}
			else{
				//				csTriples = cs.getTriples();
				for(int j = previousStart; j < triplesArray.length; j++) {
					triplesArray[j][3] = csMap.get(cs);
					//					csTriples.add(array[j]);
				}
				//array[array.length-1][3] = ucs.get(cs);
				dbECSMap.put(previousSubject, csMap.get(cs));
				//				cs.setTriples(csTriples);

			}

		}
		end = System.nanoTime();
		System.out.println("CS collection done. ucs time: " + (end-start));
	} //end createCharacteristicSetCollections


	/**
	 * Populates the this.csMapFull map which keeps mapping between a csID and all  triples ArrayList<int[4]> assigned to it.
	 */
	private int extractExtentForAllCSs() {
		long start;
		long end;

		start = System.nanoTime();
		//array[n][3] keeps unique CSid. Before processing, it sorts all triples on csID 
		Arrays.sort(triplesArray, new Comparator<int[]>() {
			public int compare(int[] s1, int[] s2) {
				if (s1[3] > s2[3])
					return 1;    // s1 comes after s2
				else if (s1[3] < s2[3])
					return -1;   // s1 comes before s2
				else {			          
					return 0;
				}
			}
		});

		ArrayList<int[]> tripleListFull = new ArrayList<int[]>();

		int csIndex = triplesArray[0][3];				
		int[] t ;
		for(int i = 0; i < triplesArray.length; i++){

			t = triplesArray[i];

			if(csIndex != t[3]){

				int[][] resultFull = new int[tripleListFull.size()][3];
				for(int ir = 0; ir < tripleListFull.size(); ir++){
					resultFull[ir] = tripleListFull.get(ir);
				}

				csMapFull.put(csIndex, resultFull);

				tripleListFull = new ArrayList<int[]>();
			}
			csIndex = t[3];

			tripleListFull.add(t);

		}		

		int[][] resultFull = new int[tripleListFull.size()][3];
		for(int i = 0; i < tripleListFull.size(); i++){
			resultFull[i] = tripleListFull.get(i);
		}


		csMapFull.put(csIndex, resultFull);

		end = System.nanoTime();
		System.out.println("CSMap Full Extent population completed. ucs2 time: " + (end-start));

		return csMapFull.size();
	}//end extractExtentForAllCSs


	/**
	 * Populates the all-, immediate-, reverseImmediate-, Ancestors maps, with the ancestors/mamas/children of each cs, the csExtentSizes with the number of tuples per cs
	 * 
	 * @return total number of triples, as the sum of the extents of all CSs
	 */
	private int extracteAncestorAndParentRelationships() {

		//init refs
		int total = 0;

		//Commented out by PV
		//		int idx, min;
		//		PushbackReader reader ;
		//		StringBuilder createTableQuery ;
		//		HashSet<Integer> propertiesMap ;
		//		HashMap<Integer, HashMap<Integer, HashSet<Integer>>> spoValues ;

		//do the merging stuff
		//System.out.println("csMap: " + csMap.toString());

		for(CharacteristicSet a_cs : csMap.keySet()){
			allCsAncestors.put(a_cs, new HashSet<CharacteristicSet>());
			int size = csMapFull.get(csMap.get(a_cs)).length;
			csExtentSizes.put(a_cs, size);
			maxCSSize = Math.max(maxCSSize, size);
		}				
		System.out.println("max CS Size: " + maxCSSize + "\n\n");

		//discover ancestry via pairwise checks
		for(CharacteristicSet parent : csMap.keySet()){

			for(CharacteristicSet child: csMap.keySet()){

				if(child.equals(parent)) continue;

				//the condition for a parent-child is that the child CS must contain ALL properties of parent   
				if(child.contains(parent)){
					if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
						//PV DIAGNOSTICS COMMENTED OUT						
						System.out.println("$$child: " + child.toString() + " parent " + parent.toString());
					}
					Set<CharacteristicSet> children = allCsAncestors.getOrDefault(parent, new HashSet<CharacteristicSet>());
					children.add(child);
					allCsAncestors.put(parent, children);
					//					csGraph.addEdge(child,parent);
				}
				//						else{
				//							//check for set inclusion?
				//							if(true) continue;
				//							if(jaccardSimilarity(child.getAsList(), parent.getAsList())>0.8){
				//								Set<NewCS> children = ancestors.getOrDefault(parent, new HashSet<NewCS>());
				//								children.add(child);
				//								ancestors.put(parent, children);
				//							}
				//						}

			}

		}
		System.out.println("Ancestor listing complete.\n\n");


		this.children = getChildren(allCsAncestors);
		for(CharacteristicSet f : csMap.keySet()){
			parents.put(f, new HashSet<CharacteristicSet>());
		}

		for(CharacteristicSet nextCS : children.keySet()){

			for(CharacteristicSet child : children.get(nextCS)){				
				Set<CharacteristicSet> set = parents.getOrDefault(child, new HashSet<CharacteristicSet>());
				set.add(nextCS);
				parents.put(child, set);
				if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
					//PV DIAGNOSTICS COMMENTED OUT		
					System.out.println("$$$child: " + child.toString() + " parent " + nextCS.toString());					
				}
			}
		}
		//System.out.println("\n\n No children: ") ;
		//		for(CharacteristicSet nextCS : csGraph) {
		for(CharacteristicSet nextCS : csMap.keySet()) {
			//if(children.containsKey(nextCS))
			//	continue;
			//System.out.println(nextCS.toString() + ": " + csSizes.get(nextCS));
			total+=csExtentSizes.get(nextCS);
			//			total+=nextCS.getTriples().size();
		}

		return total;
	} //end extracteAncestorAndParentRelationships


	/**
	 * A method that returns all immediate children for each CS   !!!! FIXED!! used to be immediateAncestors, erroneously !!!!!
	 * 
	 * @param ancestors is the graph of all CS in the data 
	 * @return a collection that for a cs provides all direct children (1-hop)    
	 */
	private static Map<CharacteristicSet, Set<CharacteristicSet>> getChildren(Map<CharacteristicSet, Set<CharacteristicSet>> ancestors){

		Map<CharacteristicSet, Set<CharacteristicSet>> immediateDescendants = new HashMap<CharacteristicSet, Set<CharacteristicSet>>();
		for(CharacteristicSet parent : ancestors.keySet()) {

			Set<CharacteristicSet> children = ancestors.get(parent) ;
			Set<CharacteristicSet> toRemove = new HashSet<CharacteristicSet>();
			for(CharacteristicSet nextChild : children) {

				for(CharacteristicSet potentialParent : children) {

					if(nextChild.equals(potentialParent)) continue;
					if(nextChild.getAsList().containsAll(potentialParent.getAsList())){
						//toBreak = true;
						//break ;
						toRemove.add(nextChild);
						break ;
					}
				}					
			}
			children.removeAll(toRemove) ;
			immediateDescendants.put(parent,children ) ;


		}

		return immediateDescendants;
	}


	/**
	 * Calculates Dense nodes and the number of their tuples. A dense CS is a CS which contains (cs.size) more triples than a threshold
	 */
	private void extractDenseNodes() {
		//		int threshold = Math.max(total/csMap.size()*meanMultiplier*2, total/100);
		//		for(CharacteristicSet nodeCS : csMap.keySet()) {
		//			if(nodeCS.getTriples().size()>=threshold) {
		//				nodeCS.setDense(true);
		//				System.out.println("Dense CS found: " + nodeCS.toString());
		//			}
		//		}

		double _DENSITY_THRESHOLD = maxCSSize*meanMultiplier/100;
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			//PV DIAGNOSTICS COMMENTED OUT		
			System.out.println("MaxCSsize: " + maxCSSize + "\tmeanMultiplier " + meanMultiplier + "\t_DNS_THETA " + _DENSITY_THRESHOLD +"\n");		
		}
		for(CharacteristicSet nextCS : csMap.keySet()) {
			//if(children.containsKey(nextCS))
			//	continue;
			//			int bb = Math.max(total/csMap.size()*meanMultiplier*2, total/100);
			//			if(csSizes.get(nextCS) >= Math.max(total/csMap.size()*meanMultiplier*2, total/100)){ //-initial Marios Implementation
			if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
				//PV DIAGNOSTICS COMMENTED OUT
				System.out.println("Candidate dense: " + nextCS.toString());			
			}
			/* ********************************************* 
			 * 		HERE IS THE DECISION ON DENSITY
			 * *********************************************/
			if(csExtentSizes.get(nextCS) >= _DENSITY_THRESHOLD){ //Dolap definition
				if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
					//PV DIAGNOSTICS 			
					System.out.println("... is dense, with extent " + csExtentSizes.get(nextCS));				
				}
				numDenseCSs++;
				denseCSs.add(nextCS);
				numDenseRows += csExtentSizes.get(nextCS);
			}

		}
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			System.out.println("Total CSs: " + csMap.size());
			//				for(NewCS nextDense : denseCSs){
			//					System.out.println("\t"+nextDense.toString());
			//				}
		}
	}// end extracteDenseNodes



	/**
	 * Finds candidate paths and sorts them over their total number of triples
	 * 
	 * 			POST-CONDITION: at the end of this method
	 * 		------------------------------------------------
	 * each Path is 
	 * a sequence of consecutive edges, in the typical graph-theoretic meaning,
	 * obligatorily starting from a dense node, and 
	 * obligatorily containing (a possibly empty) list of non-dense nodes in the rest of the path's nodes, 
	 * s.t., the list is also forming a path on the graph
	 * 
	 *  At the end of the method, the returned Set<Path> contains, for each pair of [dense node, non-dense ancestor],
	 *  a Path in the above sense that connects the ancestor to the dense descendant
	 *  
	 *  a) Collection foundCandidatePaths contains all the possible Paths, each with one dense and a list of non-dense nodes
	 *  attracted to the Path, s.t., they form a path in the traditional sense
	 *  
	 *  b) cleanup removes paths that are fully contained in other paths, i.e., if I have a 
	 *  Path=denseNode with no ancestors and  another path starting from the same denseNode with ancestors, 
	 *  then the first one is removed. This is e.g., why in cityDensity, you see 6 paths before and 4 after cleanup
	 *  
	 *  Given a & b above, The resulting paths have the following properties:
	 *  [p1] Multiplicity of paths: a dense node may "rule" multiple paths of non-dense nodes, 
	 *       i.e., two or more paths can share the same denseNode
	 *  [p2] Exclusiveness of dense nodes:  a dense node cannot belong to a path "ruled" by another dense node, 
	 *       i.e., a path cannot contain two dense nodes.
	 * 
	 * The steps taken in this method are as follows.
	 * Step1: 
	 * e.g., cs1[0,1,2,3], cs2[0,1,2,4], cs3=[0,1,2], cs4[0,1] are four CSs and cs1,cs2 are dense, 
	 * 			then 	Path1 = [cs1<--cs3<--cs4], Path2 = [cs1<--cs3]
	 * 						Path3 = [cs2<--cs3<--cs4], Path4 = [cs2<--cs3] are the possible paths from the two dense nodes  
	 * 			No other paths exist, since only cs1,cs2 are dense.
	 * The collection of paths form a strongly connected component; i.e., the (undirected) paths in the connected subgraph connect every pair of CSs in the graph   
	 *  
	 * Step 2:	Keep longest paths only Path1 = [cs1<--cs3<--cs4] and Path3 = [cs2<--cs3<--cs4]  
	 * 
	 * Step 3: sort paths based on cost DESCENDING  order. Cost of a path is the #triples contained in all CSs in path 
	 * 			If Path1.cost > Path2.cost  then Path1>>Path2>>....>>
	 * 
	 **/
	private List<Path> extractCandidatePathsSortedOnTripleNumber() {
		foundCandidatePaths = findPaths(denseCSs, pathCosts, csExtentSizes, parents, true, false);
		if (foundCandidatePaths==null || foundCandidatePaths.isEmpty()) {
			System.err.println("There are no candidate paths found. Exiting");
			System.exit(-1);
		}
		//PV DIAGNOSTICS 
		else
			if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
				System.out.println("\n[extractCandidatePathsSortedOnTripleNumber] About to process " + foundCandidatePaths.size() + " candidate paths.");
			}
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			System.out.println("---------BEFORE CLEANUP-------");
			for(Path path: foundCandidatePaths)
				System.out.println("PATH: " + path.toString());
			System.out.println("----------------\n");
		}

		Set<Path> toRemovePaths = new HashSet<Path>();
		Iterator<Path> iterOuter = foundCandidatePaths.iterator();
		while (iterOuter.hasNext()) {
			Path outerPath = iterOuter.next();
			//for(Path outerPath : foundCandidatePaths){
			Iterator<Path> iterInn = foundCandidatePaths.iterator();

			while (iterInn.hasNext()) {
				Path innerPath = iterInn.next();

				//		for(Path innerPath : foundCandidatePaths){
				if(outerPath.equals(innerPath)) continue;					
				if(innerPath.containsAll(outerPath)){
					//				foundCandidatePaths.remove(outerPath);
					toRemovePaths.add(outerPath);
					break;
				}					
			}
		}	
		foundCandidatePaths.removeAll(toRemovePaths);

		//sort paths based on cost DESCENDING  order. Cost of a path is the #triples contained in all CSs in path  
		List<Path> orderedPaths = new ArrayList<Path>(foundCandidatePaths);			
		Collections.sort(orderedPaths, new Comparator<Path>() {

			public int compare(Path o1, Path o2) {
				if (pathCosts.get(o1) > pathCosts.get(o2)) return -11; //tell sorting that if o1>o2 then o1 should come before o2
				else if (pathCosts.get(o1) < pathCosts.get(o2)) return 1; //tell sorting that if o1<o2 then o1 should come after o2
				else return 0;

			}
		});
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			//PV DIAGNOSTICS COMMENTED OUT
			System.out.println("-------AFTER CLEANUP---------");
			for(Path path: orderedPaths)
				System.out.println("PATH: " + path.toString());
			System.out.println("----------------\n");
		}
		return orderedPaths;
	}//end extracteCandidatePathsSortedOnTripleNumber()


	/**
	 * Performs merging of paths based on their cost. 
	 * POST CONDITION
	 * at the end of this step, each non-dense node is part of exactly one path
	 * 
	 * Takes an DESC order list of paths and when two paths overlap --> keeps the one with the higher cost.
	 * Two paths overlap for the common CS they contain  
	 * 
	 * Step 1-3 done in extractCandidatePathsSortedOnTripleNumber()  
	 * 
	 *  Step 4: We have Path1 = [cs1<--cs3<--cs4] and Path3 = [cs2<--cs3<--cs4], with Path1.cost>Path2.cost 
	 *  		then remove from Path3 all cs that exist in Path1, i.e., paths become Path1 = [cs1<--cs3<--cs4] and Path3 = [cs2]
	 *  		It removes only non-dense nodes, since a dense node can only belong to a single path.
	 *  	Step 4 also updates the cardinality, i.e., cost,  of the small Path in order to remove the costs of the CSs that were removed and reorders the remaining Path list such that the small path is positioned lower in the order list
	 *  	
	 * @param orderedPaths
	 */
	private void removeNestedAndEmptyPaths(List<Path> orderedPaths) {
		Path bigPath ;

		int totalIterations = 0;					
		//get each path based on their costs
		for(int i = 0; i < orderedPaths.size(); i++){
			//get the path with the highest cost
			bigPath = orderedPaths.get(i);
			// iterate over the rest
			for(int k = i+1; k < orderedPaths.size(); k++){
				//get the next less costly
				Path smallPath = orderedPaths.get(k);
				//remove from the less costly the CS that are already contained in the most costly path. 
				smallPath.removeAll(bigPath);
				//update the cost of the lessCostly to remove the triples contained in the CS that were removed.
				updateCardinality(smallPath, csExtentSizes, pathCosts);	
			}				


			//recalculate ordering based on updated costs before moving to the next path
			Collections.sort(orderedPaths.subList(i+1, orderedPaths.size()), new Comparator<Path>() {
				public int compare(Path o1, Path o2) {
					if (pathCosts.get(o1) > pathCosts.get(o2)) return -1;
					else if (pathCosts.get(o1) < pathCosts.get(o2)) return 1;
					else return 0;

				}
			});

		}
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			//PV DIAGNOSTICS 
			System.out.println("-------------------------------------\n\n");
			for(Path path: orderedPaths) {
				System.out.println("Survives Nested Removal: " + path.toString());
			}
		}

		//remove empty paths
		Iterator<Path> finalIt = orderedPaths.iterator();
		while(finalIt.hasNext()){
			Path n = finalIt.next();
			if(n.isEmpty()){
				finalIt.remove();
				pathCosts.remove(n);
			}
			else{
				updateCardinality(n, csExtentSizes, pathCosts);
				finalListCandidatePAths.add(n);
			}
		}
		for(Path finalCS : finalListCandidatePAths){
			finalUniqueCandidatePathsMap.addAll(finalCS);
		}
		if(ModeOfWork.mode == WorkMode.DEBUG_GLOBAL) {
			System.out.println("After removalOfNestedAndEmptyPaths,finalUniqueCandidatePathsMap.size: " + finalUniqueCandidatePathsMap.size());
			System.out.println("finalListCandidatePAths size is: " + finalListCandidatePAths.size() + "...\n\t.. and the actual paths: " + finalListCandidatePAths.toString());
			System.out.println("-------------------------------------\n\n");
		}
	}//end method removeNestedAndEmptyPaths









	/***
	 * Takes as input a path and updates its cost based on the CS it contains.  
	 * 
	 * @param path is the path containing a list of CS 
	 * @param csSizes contains the size (#triples) in each cs 
	 * @param pathCosts contains the size (#triples) in all CS of a path.
	 * @return updated cost of the input path.
	 */
	private static Map<Path, Integer> updateCardinality(Path path,
			Map<CharacteristicSet, Integer> csSizes, Map<Path, Integer> pathCosts) {
		int newCardinality = 0;

		for(CharacteristicSet innerCS : path){

			newCardinality += csSizes.get(innerCS);

		}
		pathCosts.put(path, newCardinality) ;

		return pathCosts;

	}



	/***
	 * Computes a set of paths. A path is a list of CS between a dense node and its ancestors
	 * e.g., cs1[0,1,2,3], cs2[0,1,2,4], cs3=[0,1,2], cs4[0,1] are four CSs and cs1,cs2 are dense, 
	 * 			then 	Path1 = [cs1<--cs3<--cs4], Path2 = [cs1<--cs3]
	 * 					Path3 = [cs2<--cs3<--cs4], Path4 = [cs2<--cs3] are the possible paths from the two dense nodes  
	 * 			No other paths exist, since only cs1,cs2 are dense.
	 * The collection of paths form a strongly connected component; i.e., the (undirected) paths in the connected subgraph connect every pair of CSs in the graph   
	 *  
	 *  a1. The collection foundPaths, creates a path starting from a denseNode and 
	 *  moving upwards until it reaches another dense node or null root (empty stack). 
	 *  The condition " if(denseCheck && denseCSs.contains(parent)) {" does this work.
	 *  
	 *  a2. Moving upwards in a lattice, the method creates multiple paths starting from a denseNode.
	 *  
	 *  a3. If a denseNode has an immediate dense parent, then it creates a new path only for the child denseNode 
	 *  and starts a new path from its parent...
	 *
	 * @param denseCSs are the dense cs
	 * @param pathCosts the cost of a path between two CSs. The cost of a path is the cardinality , i.e., the no of triples contained in ALL cs in the path 
	 * @param csSizes the no of triples a cs contains
	 * @param parents a collection with mappings from a parent cs to its child
	 * @param denseCheck
	 * @param withSiblings
	 * @return foundPaths is a Set containing mapping between a dense CS and a CS in the ancestor graph for which a path exists!
	 */
	public static Set<Path> findPaths(Set<CharacteristicSet> denseCSs, 
			Map<Path, Integer> pathCosts, 
			Map<CharacteristicSet, Integer> csSizes, 
			Map<CharacteristicSet, Set<CharacteristicSet>> immediateAncestors, 
			boolean denseCheck,
			boolean withSiblings)
	{
		Stack<Path> stack ;
		Path path ;
		Path curPath ;
		CharacteristicSet curCS ;
		int cardinality ;
		Path newCur ;
		Set<Path> foundPaths = new HashSet<Path>();

		//create a path from each denseCS, a path is a ArrayList<CharacteristicSet>
		for(CharacteristicSet nextDenseCS :  denseCSs){ 

			//stack for keeping paths
			stack = new Stack<Path>();
			path =  new Path();

			path.add(nextDenseCS);
			stack.push(path);		
			Set<CharacteristicSet> visited = new HashSet<CharacteristicSet>();

			while(!stack.empty()){

				// get a path from stack
				curPath = stack.pop();
				//get the last cs from path 
				curCS = curPath.get(curPath.size()-1);

				if(withSiblings && visited.contains(curCS)) continue;


				//if no parents, is root, add path
				if(immediateAncestors.get(curCS).isEmpty()){
					//no parents and no dense node reached.
					//has it become dense?
					cardinality = 0;
					for(CharacteristicSet node : curPath){
						cardinality += csSizes.get(node);
						//added.add(node);
					}
					foundPaths.add(curPath);
					pathCosts.put(curPath, cardinality);

					continue;
				}

				if(!immediateAncestors.get(curCS).isEmpty()){

					for(CharacteristicSet parent : immediateAncestors.get(curCS)){							
						if(denseCheck && denseCSs.contains(parent)) {
							//System.out.println("@@@@@cur: " + curCS.toString() + "\t@@@@@par: " + parent.toString() + "\n");
							//it already contains a dense node so just add it.
							foundPaths.add(curPath);

							cardinality = 0;
							for(CharacteristicSet node : curPath){
								cardinality += csSizes.get(node);
							}
							pathCosts.put(curPath, cardinality);

							continue;
						}
						newCur =  new Path(curPath);
						newCur.add(parent);	
						if(withSiblings)
							visited.add(parent);
						stack.push(newCur);
					}
				}

			}

		}
		return foundPaths;

	}//end findPaths

	static private double jaccardSimilarity(List<Integer> a1, List<Integer> b1) {
		Set<Integer> a = new HashSet<Integer>(a1);
		Set<Integer> b = new HashSet<Integer>(b1);
		final int sa = a.size();
		final int sb = b.size();
		a.retainAll(b);
		final int intersection = a.size();
		return 1d / (sa + sb - intersection) * intersection;
	}

	protected static <T> List<List<T>> cartesianProduct(List<List<T>> lists) {
		List<List<T>> resultLists = new ArrayList<List<T>>();
		if (lists.size() == 0) {
			resultLists.add(new ArrayList<T>());
			return resultLists;
		} else {
			List<T> firstList = lists.get(0);
			List<List<T>> remainingLists = cartesianProduct(lists.subList(1, lists.size()));
			for (T condition : firstList) {
				for (List<T> remainingList : remainingLists) {
					ArrayList<T> resultList = new ArrayList<T>();
					resultList.add(condition);
					resultList.addAll(remainingList);
					resultLists.add(resultList);
				}
			}
		}
		return resultLists;
	}



	/**
	 * A black-box method that creates the tables and adds data. 
	 * 
	 * TODO REFACTOR THIS MEGA-METHOD!!!!
	 * 
	 * @param dbECSMap
	 * @param reverseCSMap
	 * @param pathMap
	 * @param csToPathMap
	 * @param reversePathMap
	 * @param tripleGroups
	 * @return 0 if successful, -1 if not successful
	 */
	private int createTablesPopulateDatabase(Map<Integer, Integer> dbECSMap,
			Map<Integer, CharacteristicSet> reverseCSMap, Map<Path, Integer> pathMap,
			Map<CharacteristicSet, Path> csToPathMap, Map<Integer, Path> reversePathMap,
			Iterator<Map.Entry<Integer, int[][]>> tripleGroups) {

		int idx;
		int min;
		PushbackReader reader;
		StringBuilder createTableQuery;
		HashMap<Integer, HashMap<Integer, HashSet<Integer>>> spoValues;
		/**
		 * Create database tables
		 *    
		 */

		/**
		 * Creation of a dictionary table with a row for each distinct subject and object o  
		 */
		Statement stmt = null;
		StringBuilder sb2 = new StringBuilder();
		CopyManager cpManager2;
		System.out.println("Adding keys to dictionary. " + new Date().toString());
		try {
			if(this.conn == null) {
				System.err.println("Lost db Connection; exiting...");
				System.exit(-1);
			}
			stmt = this.conn.createStatement();
			//HashCode URIs 
			//stmt.executeUpdate("CREATE TABLE IF NOT EXISTS dictionary (id INT, label INT); ");
			//plain string URIs 
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS dictionary (id INT, label TEXT); ");

			stmt.close();		       


			cpManager2 = ((PGConnection)conn).getCopyAPI();
			PushbackReader reader2 = new PushbackReader( new StringReader(""), 100000 );
			Iterator<Map.Entry<String, Integer>> keyIt = intMap.entrySet().iterator();
			int iter = 0;
			while(keyIt.hasNext())
			{
				Entry<String, Integer> nextEntry = keyIt.next();
				//append hashcoded values
				//				sb2.append(nextEntry.getValue()).append(",").append(nextEntry.getKey().hashCode()).append("\n");
				//append string values
				sb2.append(nextEntry.getValue()).append(",").append(nextEntry.getKey()).append("\n");
				if (iter++ % batchSize == 0)
				{
					reader2.unread( sb2.toString().toCharArray() );
					cpManager2.copyIn("COPY dictionary FROM STDIN WITH CSV", reader2 );
					sb2.delete(0,sb2.length());
				}
				keyIt.remove();
			}
			reader2.unread( sb2.toString().toCharArray() );
			cpManager2.copyIn("COPY dictionary FROM STDIN WITH CSV", reader2 );
			reader2.close();
		} catch (SQLException e2) {
			e2.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Done adding keys to dictionary. " + new Date().toString());


		/**
		 * Create database table cs schema. CS schema contains the id of a CS and the array of properties (int[])
		 */
		HashSet<String> pathPairs = new HashSet<String>();
		HashMap<String, Set<Integer>> pathPairProperties = new HashMap<String, Set<Integer>>();
		HashMap<Integer, int[]> csProps = new HashMap<Integer, int[]>(); 


		CopyManager cpManager;
		try {
			cpManager = ((PGConnection)conn).getCopyAPI();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return -1;
		}

		int nextPathIndex;
		int multiIndex = -1;								

		while(tripleGroups.hasNext()){
			Entry<Integer, int[][]> nextEntry = tripleGroups.next();
			nextPathIndex = nextEntry.getKey();
			int[][] triplesArray = nextEntry.getValue();
			//System.out.println("Next CS: " + nextPathIndex + ", " + reversePathMap.get(nextPathIndex));
			//if(triplesArray == null || triplesArray.length == 0)
			//System.out.println("Empty triples set???");


			//propertiesMap = new HashSet<Integer>();
			List<Integer> propsList = new ArrayList<Integer>();

			for(CharacteristicSet n : reversePathMap.get(nextPathIndex)){
				for(Integer np : n.getAsList()){
					if(!propsList.contains(np))
						propsList.add(np);
				}
			}


			Collections.sort(propsList);

			String cs_properties_query = "CREATE TABLE IF NOT EXISTS cs_schema (id INT, properties integer[]); INSERT INTO cs_schema (id, properties) VALUES ";
			cs_properties_query += "( " + nextPathIndex + ", "; 

			int[] props ;


			props = new int[propsList.size()];
			int propIdx = 0;
			//					ArrayList<Integer> sortedProperties = new ArrayList<Integer>(propertiesMap);
			//					Collections.sort(sortedProperties);
			//createTableQuery = new StringBuilder();
			//createTableQuery.append("CREATE TABLE IF NOT EXISTS cs_" + nextPathIndex + " (s INT, ");
			for(int property : propsList){
				//createTableQuery.append("p_"+property + " INT, ");
				props[propIdx++] = property;
			}
			csProps.put(nextPathIndex, props);
			cs_properties_query += "ARRAY" + Arrays.toString(props) + ") ";
			//createTableQuery.deleteCharAt(createTableQuery.length()-2);
			//createTableQuery.append(')');
			//createTableQuery.append(';');


			StringBuilder sb = new StringBuilder();									

			try{											

				HashMap<Integer, HashSet<Integer>> poValues ;
				HashSet<Integer> oValues ;
				int[] valueArray ;
				spoValues = new HashMap<Integer, HashMap<Integer, HashSet<Integer>>>();
				//System.out.println("# of Triples: " + triplesArray.length);

				//let's try sorting to speed things up
				Arrays.sort(triplesArray, new Comparator<int[]>() {
					public int compare(int[] s1, int[] s2) {
						if (s1[0] > s2[0])
							return 1;    // s1 comes after s2
						else if (s1[0] < s2[0])
							return -1;   // s1 comes before s2
						else {	
							return 0;
						}
					}
				});
				int prevSubject = triplesArray[0][0];
				sb.append(prevSubject).append(',');
				poValues = new HashMap<Integer, HashSet<Integer>>();
				int rowBatch = 0;
				multiValuedProperties = new HashSet<Integer>();
				//StringBuilder stringHash ;
				long sp ;
				Set<Long> spSet = new HashSet<Long>();
				for(int[] tripleNext : triplesArray){
					//stringHash.append(tripleNext[0]).append("_").append(tripleNext[1]);
					sp = (((long)tripleNext[0]) << 32) | (tripleNext[1] & 0xffffffffL);

					if(spSet.contains(sp))
						multiValuedProperties.add(tripleNext[1]);
					spSet.add(sp);
				}
				createTableQuery = new StringBuilder();
				createTableQuery.append("CREATE TABLE IF NOT EXISTS cs_" + nextPathIndex + " (s INT, ");
				for(int property : propsList){
					if(!multiValuedProperties.contains(property))
						createTableQuery.append("p_"+property + " INT, ");
					else{
						createTableQuery.append("p_"+property + " INT[], ");
					}

				}												
				createTableQuery.deleteCharAt(createTableQuery.length()-2);
				createTableQuery.append(')');
				createTableQuery.append(';');
				this.createTableStatements.put(createTableQuery.toString(),Integer.toString(nextPathIndex));
				try{				
					//c.setAutoCommit(false);
					stmt = conn.createStatement();

					stmt.executeUpdate(createTableQuery.toString());
					stmt.close();		       

				} catch (Exception e){
					e.printStackTrace();
					return -1;
				}

				if(!multiValuedProperties.isEmpty()){
					String multiValued = "CREATE TABLE IF NOT EXISTS multi_valued (cs int, p int); ";
					String multiValuedValues = "";
					for(Integer mp : multiValuedProperties){
						multiValuedValues += "("+nextPathIndex+", "+mp+"), ";
					}
					multiValuedValues = multiValuedValues.substring(0, multiValuedValues.length()-2) + "; ";
					try{				
						//c.setAutoCommit(false);
						stmt = conn.createStatement();
						stmt.executeUpdate(multiValued+" INSERT INTO multi_valued (cs, p) VALUES " + multiValuedValues+"; ");
						stmt.close();		       

					} catch (Exception e){
						e.printStackTrace();
						return -1;
					}
				}


				// populate CS tables.
				for(int[] tripleNext : triplesArray){

					if(prevSubject != tripleNext[0]){
						//wrap up and go to next subject							

						for(int nextProperty : propsList){									
							if(poValues.containsKey(nextProperty)){
								if(poValues.get(nextProperty).size() > 1){
									//multisb.append(prevSubject).append(",").append(nextProperty).append(",");
									valueArray = new int[poValues.get(nextProperty).size()];
									idx = 0;
									min = Integer.MAX_VALUE;
									List<Integer> multiString = new ArrayList<Integer>();
									for(Integer nextObject : poValues.get(nextProperty)){
										valueArray[idx++] = nextObject;
										//min = Math.min(min, nextObject);												
										multiString.add(nextObject);												

									}
									//sb.append(min).append(",");
									int[] integerArray = multiString.stream().mapToInt(i->i).toArray();
									String arrS = Arrays.toString(integerArray).replace('[', '{').replace(']', '}');
									sb.append("\""+arrS+"\"").append(",");

								}
								else{
									for(Integer nextObject : poValues.get(nextProperty)){
										if(multiValuedProperties.contains(nextProperty)){
											sb.append("\"{"+nextObject+"}\"").append(",");
										}
										else
											sb.append(nextObject).append(",");
									}
								}
							}
							else{
								sb.append("null").append(",");
							}

						}						
						sb.deleteCharAt(sb.length()-1);
						sb.append("\n");
						if (rowBatch++ % batchSize == 0)
						{
							reader = new PushbackReader( new StringReader(""), sb.length() );
							reader.unread( sb.toString().toCharArray() );
							cpManager.copyIn("COPY cs_" + nextPathIndex + " FROM STDIN WITH CSV NULL AS 'null'", reader );
							sb.delete(0,sb.length());
							if (rowBatch++ % 1000000 == 0)
								System.out.println("Next checkpoint: " + rowBatch);
						}
						poValues = new HashMap<Integer, HashSet<Integer>>();						
						sb.append(tripleNext[0]).append(',');
					}
					oValues = poValues.getOrDefault(tripleNext[1], new HashSet<Integer>());
					oValues.add(tripleNext[2]);			
					if(dbECSMap.containsKey(tripleNext[2])){
						CharacteristicSet refCS = reverseCSMap.get(dbECSMap.get(tripleNext[2]));
						//System.out.println("1"  + rucs.get(dbECSMap.get(tripleNext[2])));
						//System.out.println("2" + csToPathMap.get(rucs.get(dbECSMap.get(tripleNext[2]))));
						if(csToPathMap.containsKey(refCS)){
							Path chkPath = csToPathMap.get(refCS);
							Integer pairedPathIndex = pathMap.get(chkPath);
							//int pairedPathIndex = pathMap.get(csToPathMap.get(reverseCSMap.get(dbECSMap.get(tripleNext[2]))));
							if (pairedPathIndex == null) {
								System.err.println("\t^^^^CS^^^ " + refCS.toString());
								System.err.println("\t^^P^^ " + chkPath.toString());
								System.err.println("\t^^PPI^^ is NULL");
							}
							else {
								String pairString = ""+nextPathIndex +"_"+pairedPathIndex;
								pathPairs.add(pairString);
								Set<Integer> ecsProp = pathPairProperties.getOrDefault(pairString, new HashSet<Integer>());
								ecsProp.add(tripleNext[1]) ;
								pathPairProperties.put(pairString, ecsProp) ;																																			
							}
						}

					}
					poValues.put(tripleNext[1], oValues);
					prevSubject = tripleNext[0];

				}

				for(int nextProperty : propsList){
					if(poValues.containsKey(nextProperty)){
						if(poValues.get(nextProperty).size() > 1){

							valueArray = new int[poValues.get(nextProperty).size()];
							idx = 0;
							min = Integer.MAX_VALUE;
							List<Integer> multiString = new ArrayList<Integer>();
							for(Integer nextObject : poValues.get(nextProperty)){
								valueArray[idx++] = nextObject;
								//min = Math.min(min, nextObject);												
								multiString.add(nextObject);												

							}										
							//insert?									
							//sb.append(min).append(",");
							int[] integerArray = multiString.stream().mapToInt(i->i).toArray();
							String arrS = Arrays.toString(integerArray).replace('[', '{').replace(']', '}');
							sb.append("\""+arrS+"\"").append(",");

						}
						else{
							for(Integer nextObject : poValues.get(nextProperty)){
								if(multiValuedProperties.contains(nextProperty)){
									sb.append("\"{"+nextObject+"}\"").append(",");
								}
								else
									sb.append(nextObject).append(",");
							}
						}

					}
					else{																																		
						sb.append("null").append(",");
					}


				}	
				//last line
				sb.deleteCharAt(sb.length()-1);
				sb.append("\n");

				reader = new PushbackReader( new StringReader(""), sb.length() );
				reader.unread( sb.toString().toCharArray() );
				cpManager.copyIn("COPY cs_" + nextPathIndex + " FROM STDIN WITH CSV NULL AS 'null'", reader );
				sb.delete(0,sb.length());


				//create gin indexes
				for(Integer mp : multiValuedProperties){
					//multiValuedValues += "("+nextPathIndex+", "+mp+"), ";
					String ginIndexS = " CREATE INDEX cs"+nextPathIndex+"_p"+mp+"_gin ON cs_"+nextPathIndex+" USING gin (p_"+mp+") ;";
					try{				
						//c.setAutoCommit(false);
						stmt = conn.createStatement();
						stmt.executeUpdate(ginIndexS);
						stmt.close();	       

					} catch (Exception e){
						e.printStackTrace();
					}
				}

				//					    reader = new PushbackReader( new StringReader(""), multisb.length() );
				//					    reader.unread( multisb.toString().toCharArray() );
				//					    cpManager.copyIn("COPY spo FROM STDIN WITH CSV NULL AS 'null'", reader );
				//					    multisb.delete(0,multisb.length());


				tripleGroups.remove();


				//System.out.println("Removed CS from cs Map.");

			}
			catch (Exception e){
				e.printStackTrace();
			}

			try{				
				Statement stmt2 = conn.createStatement();
				stmt2.executeUpdate(cs_properties_query);
				stmt2.close();

			} catch (Exception e){
				e.printStackTrace();
			}



		}				 				   


		System.out.println(pathPairs.size());
		StringBuilder ecsQuery = new StringBuilder();

		ecsQuery.append("CREATE TABLE IF NOT EXISTS ecs_schema (id INT, css INT, cso INT, css_properties int[], cso_properties int[]); ");
		try{				
			Statement stmt2 = conn.createStatement();
			stmt2.executeUpdate(ecsQuery.toString());
			stmt2.close();

		} catch (Exception e){
			e.printStackTrace();
		}


		//ecsQuery.append("INSERT INTO ecs_schema (id, css, cso, css_properties, cso_properties) VALUES ");
		try{
			CopyManager cpManagerIndex = ((PGConnection)conn).getCopyAPI();
			PushbackReader reader2 = null;
			sb2 = new StringBuilder();

			idx = 0;
			for(String csPair : pathPairs){
				String[] split = csPair.split("_");
				sb2.append(idx++).append(",").append(split[0]).append(",").append(split[1]).append(",").
				append("\""+Arrays.toString(csProps.get(Integer.parseInt(split[0]))).replace('[', '{').replace(']', '}')+"\"").append(",").
				append("\""+Arrays.toString(csProps.get(Integer.parseInt(split[1]))).replace('[', '{').replace(']', '}')+"\"").
				append("\n");

				if (idx++ % batchSize == 0)
				{
					reader2	= new PushbackReader( new StringReader(""), sb2.length());
					reader2.unread( sb2.toString().toCharArray() );
					cpManagerIndex.copyIn("COPY ecs_schema FROM STDIN WITH CSV", reader2 );
					sb2.delete(0,sb2.length());
					reader2.close();
				}
				Set<Integer> props = pathPairProperties.get(csPair) ;
				for(Integer nextProp : props){

					if(!multiValuedProperties.contains(nextProp)){
						String index = " CREATE INDEX IF NOT EXISTS cs_"+split[0]+"_p"+nextProp+" ON cs_"+split[0]+" (p_"+nextProp+") " ;							

						stmt = conn.createStatement();
						stmt.executeUpdate(index);
						stmt.close();	
					}								      

				}
				String index = " CREATE INDEX IF NOT EXISTS cs_"+split[1]+"_s ON cs_"+split[1]+" (s) " ;

				stmt = conn.createStatement();
				stmt.executeUpdate(index);
				stmt.close();	       

			}
			//last ecs
			if (sb2.length()>0) {
				reader2	= new PushbackReader( new StringReader(""), sb2.length());
				reader2.unread( sb2.toString().toCharArray() );
				cpManagerIndex.copyIn("COPY ecs_schema FROM STDIN WITH CSV", reader2 );
				sb2.delete(0,sb2.length());
				reader2.close();}


		} catch (Exception e){
			e.printStackTrace();
		}

		String propertiesSetQuery = "CREATE TABLE IF NOT EXISTS propertiesSet (id INT, uri TEXT) ; "
				+ "INSERT INTO propertiesSet (id, uri) VALUES ";
		int propCount = 0;
		for(int nextProp : revPropertiesSet.keySet()){
			propertiesSetQuery += "(" + nextProp + ", '" + revPropertiesSet.get(nextProp) + "') ";
			if(propCount < revPropertiesSet.size()-1)
				propertiesSetQuery += ", ";
			else
				propertiesSetQuery += "; ";
			propCount++;
		}
		//System.out.println(propertiesSetQuery);
		try{				
			//c.setAutoCommit(false);
			stmt = conn.createStatement();
			stmt.executeUpdate(propertiesSetQuery.toString());
			stmt.close();	       

		} catch (Exception e){
			e.printStackTrace();
		}
		//List<List<Integer>> sortECSList = new List<Integer>(cs);



		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}//end createdDBPopulateTables()



}//end class
