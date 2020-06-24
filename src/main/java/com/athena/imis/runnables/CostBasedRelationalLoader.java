package com.athena.imis.runnables;

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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang3.ArrayUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.athena.imis.models.CharacteristicSet;
import com.athena.imis.models.DirectedGraph;
import com.athena.imis.models.Path;

import gnu.trove.map.hash.THashMap;

public class CostBasedRelationalLoader  {

	public static Map<String, Integer> propertiesSet = new THashMap<String, Integer>();
	public static Map<Integer, String> revPropertiesSet = new THashMap<Integer, String>();
	public static Set<Integer> multiValuedProperties ;
	public static Map<String, Integer> intMap ;
	public static Map<Integer, String> revIntMap = new THashMap<Integer, String>();
	public static int meanMultiplier = 1;

	private static final Logger LOG = LogManager.getLogger(CostBasedRelationalLoader.class);

	public static void main(String[] args) {


		//195.251.63.129
		/*localhost
		C:/temp/lubm1.nt
		testbatch
		100
		postgres
		dolap*/

		/**
		 * Database Creation
		 */

		
		// TODO Add logger  
		//	LOG.debug("Starting time: " + new Date().toString());

		System.out.println("Starting time: " + new Date().toString());
		int batchSize = Integer.parseInt(args[3]);
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName("org.postgresql.Driver");

			conn = DriverManager
					.getConnection("jdbc:postgresql://"+args[0]+":5432/", args[4], args[5]);

			Statement cre = conn.createStatement();
			cre.executeUpdate("DROP DATABASE IF EXISTS "+args[2]+" ;");	         

			cre.executeUpdate("CREATE DATABASE "+args[2]+" ;");
			cre.close();
			conn.close();
			conn = DriverManager
					.getConnection("jdbc:postgresql://"+args[0]+":5432/" + args[2], args[4], args[5]);			         			        				

			System.out.println("Opened database successfully");

		} catch ( Exception e ) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			System.exit(0);
		}	      

		if(args.length > 6)
			meanMultiplier = Integer.parseInt(args[6]);

		/**
		 * Read Input .nt File and stores triples in an int array[#triples][4] --> [s, p, o, csId], csID initialized to -1  (it is filled later when CS are constructed)
		 * 				an auto-increment is assigned to {s, o} (s,o share the same index) and p based on the order of occurrence
		 * 				
		 * 				e.g., for 3  n-triples <s1 p1 o1>. <s1 p2 o2>. <s3 p1 o1>.
		 *  							array[0][0] = [0,0,1,-1] --> <s1 p1 o1>
		 *  							array[0][1] = [0,1,2,-1] --> <s1 p2 o2>
		 *  							array[0][2] = [3,0,1,-1] --> <s3 p1 o1>
		 */

		//create indices 
		// no of triples in file
		int triplesParsed2 = 0;
		// index for array of triples 
		int next = 0; 
		//index for no of properties in triples
		int propIndex = 0;
		//index for no of nodes, i.e., (s) or (o) in triples.
		int  nodeIndex = 0;  

		//log start time
		long start = System.nanoTime();

		FileInputStream is;
		try {
			is = new FileInputStream(args[1]);
			NxParser nxp = new NxParser();
			//RdfXmlParser nxp = new RdfXmlParser(); 
			//nxp.parse(is, "http://ex");
			nxp.parse(is);
			for (Node[] nx : nxp){
				triplesParsed2++;
				//if(triplesParsed2 == 1000000) break;
				// prints the subject, eg. <http://example.org/>
				//System.out.println(nx[0] + " " + nx[1] + " " + nx[2]);
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 	

		System.out.println("triplesParsed: " + triplesParsed2);
		System.out.println(System.nanoTime()-start);
		final int[][] array = new int[triplesParsed2][4];
		intMap = new THashMap<String, Integer>(triplesParsed2);
		String s, p, o;

		try {
			int[] ar ;
			is = new FileInputStream(args[1]);
			NxParser nxp = new NxParser();
			//RdfXmlParser nxp = new RdfXmlParser(); 
			//nxp.parse(is, "http://ex");
			nxp.parse(is);
			for (Node[] nx : nxp){
				if(triplesParsed2==0) break;
				triplesParsed2--;
				// prints the subject, eg. <http://example.org/>
				//System.out.println(nx[0] + " " + nx[1] + " " + nx[2]);
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
				//array.add(next, ar);
				array[next++] = ar;
				//next++;
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		long end = System.nanoTime();

		System.out.println("piped: " + (end-start));

		/**
		 * Sort the array of triples in ascending order of subject node.   
		 */
		Arrays.sort(array, new Comparator<int[]>() {
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

		/**
		 * Iterate over the triples, Create Characteristics CSs and store them in 3 HashMaps
		 * CSMap<CharacteristicSet,Integer> : assigns an index (e.g., csID) to each cs; a CS is a list of properties {p1,p2,...,pn} (int representation)
		 * reverseCSMap<Integer,CharacteristicSet>: keeps the reverse mapping from index (csID) to a CS  
		 * dbECSMap: keeps a mapping between a subject index and a csID
		 */

		int previousSubject = Integer.MIN_VALUE;

		// characteristic set  map --- assigns a int to each distinct cs; a CS is a list of properties (int representation)
		Map<CharacteristicSet, Integer> csMap = new HashMap<>();
		
		//gp addition for grpah-based management of CS
//		DirectedGraph<CharacteristicSet> csGraph = new DirectedGraph<CharacteristicSet>();
//		ArrayList<int[]> csTriples;
		
		
		int csIndex = 0;
		/*for(int i = 0; i < l.size(); i++){
					long t = l.apply((long)i);*/
		int previousStart = 0;
		CharacteristicSet cs = null;
		
		int[] t ;
		int subject ;
		int prop ;

		//keeps a mapping between a subject int and a csID  
		Map<Integer, Integer> dbECSMap = new THashMap<Integer, Integer>(array.length/10);
		
		//reverse characteristic set  map
		Map<Integer, CharacteristicSet> reverseCSMap = new HashMap<Integer, CharacteristicSet>();

		List<Integer> propList = new ArrayList<Integer>();;

		//array is sorted on s
		for(int i = 0; i < array.length; i++){
			subject = array[i][0];
			prop = array[i][1];

			// if the subject has changed check if a new CS is needed 
			if(i > 0 && previousSubject != subject){

				cs = new CharacteristicSet(propList);	
				 		
				if(!csMap.containsKey(cs)){
//					csTriples = new ArrayList<int[]>();
					dbECSMap.put(previousSubject, csIndex);
					reverseCSMap.put(csIndex, cs);
					//update with the new csID the triples array for all triples mapped to this CS
					for(int j = previousStart; j < i; j++) {
						array[j][3] = csIndex;
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
						array[j][3] = csMap.get(cs);
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
				for(int j = previousStart; j < array.length; j++) {
					array[j][3] = csIndex;
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
				for(int j = previousStart; j < array.length; j++) {
					array[j][3] = csMap.get(cs);
//					csTriples.add(array[j]);
				}
				//array[array.length-1][3] = ucs.get(cs);
				dbECSMap.put(previousSubject, csMap.get(cs));
//				cs.setTriples(csTriples);
				
			}

		}
		end = System.nanoTime();
		System.out.println("ucs time: " + (end-start));
		
		/**
		 * Construct a utility CS full map  
		 * It constructs a csMapFull  which keeps mapping between a csID and all  triples ArrayList<int[4]> assigned to it.
		 */
		
		
		start = System.nanoTime();
		//array[n][3] keeps unique CSid. Before processing, it sorts all triples on csID 
		Arrays.sort(array, new Comparator<int[]>() {
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

		Map<Integer, int[][]> csMapFull = new HashMap<Integer, int[][]>();

		csIndex = array[0][3];				

		for(int i = 0; i < array.length; i++){

			t = array[i];

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
		System.out.println("ucs2 time: " + (end-start));

		System.out.println("csMapFull size: " + csMapFull.size());


		

		/**
		 * Process CSs for deriving ancestor relationships   
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
			return;
		}

		//init refs
		int idx, min, total = 0;
		PushbackReader reader ;
		StringBuilder createTableQuery ;
		HashSet<Integer> propertiesMap ;
		HashMap<Integer, HashMap<Integer, HashSet<Integer>>> spoValues ;

		//do the merging stuff
		//System.out.println("csMap: " + csMap.toString());
		
		// holds all ancestor of a CS
		Map<CharacteristicSet, Set<CharacteristicSet>> ancestors = new HashMap<CharacteristicSet, Set<CharacteristicSet>>();
		//holds the number of triples (size) of a CS
		Map<CharacteristicSet, Integer> csSizes = new HashMap<CharacteristicSet, Integer>();
		
		//Initialize ancestor map and size map
		int maxCSSize = Integer.MIN_VALUE;
		for(CharacteristicSet a_cs : csMap.keySet()){
			ancestors.put(a_cs, new HashSet<CharacteristicSet>());
			int size = csMapFull.get(csMap.get(a_cs)).length;
			csSizes.put(a_cs, size);
			maxCSSize = Math.max(maxCSSize, size);
		}				
		System.out.println("max CS Size: " + maxCSSize);
		//discover ancestry

		for(CharacteristicSet parent : csMap.keySet()){

			for(CharacteristicSet child: csMap.keySet()){

				if(child.equals(parent)) continue;

				//the condition for a parent-child is that the child CS must contain ALL properties of parent   
				if(child.contains(parent)){
					Set<CharacteristicSet> children = ancestors.getOrDefault(parent, new HashSet<CharacteristicSet>());
					children.add(child);
					ancestors.put(parent, children);
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
		System.out.println("Ancestor listing complete.");
		
		
		//Create two additional collections for holding ancestor relations
		// ***Immediate ancestors are direct (1-hop) parents of a CS   
		// ***Reverse immediate ancestors are direct (1-hop) children of a CS   
		Map<CharacteristicSet, Set<CharacteristicSet>> immediateAncestors = getImmediateAncestors(ancestors);
		Map<CharacteristicSet, Set<CharacteristicSet>> reverseImmediateAncestors = new HashMap<CharacteristicSet, Set<CharacteristicSet>>();
		for(CharacteristicSet f : csMap.keySet()){
			reverseImmediateAncestors.put(f, new HashSet<CharacteristicSet>());
		}

		for(CharacteristicSet nextCS : immediateAncestors.keySet()){

			for(CharacteristicSet child : immediateAncestors.get(nextCS)){
				Set<CharacteristicSet> set = reverseImmediateAncestors.getOrDefault(child, new HashSet<CharacteristicSet>());
				set.add(nextCS);
				reverseImmediateAncestors.put(child, set);
			}
		}
		//System.out.println("\n\n No children: ") ;
//		for(CharacteristicSet nextCS : csGraph) {
		for(CharacteristicSet nextCS : csMap.keySet()) {
			//if(immediateAncestors.containsKey(nextCS))
			//	continue;
			//System.out.println(nextCS.toString() + ": " + csSizes.get(nextCS));
			total+=csSizes.get(nextCS);
//			total+=nextCS.getTriples().size();
		}
		System.out.println("total estimate size: " + total) ;
		System.out.println("mean size: " + total/csMap.size()) ;
		
		/**
		 * Process CSs for finding dense CSs.
		 * A dense CS is a CS which contains (cs.size) more triples than a threshold    
		 */
//		int threshold = Math.max(total/csMap.size()*meanMultiplier*2, total/100);
//		for(CharacteristicSet nodeCS : csMap.keySet()) {
//			if(nodeCS.getTriples().size()>=threshold) {
//				nodeCS.setDense(true);
//				System.out.println("Dense CS found: " + nodeCS.toString());
//			}
//		}
		
		int noOfdenseCS = 0, totalDenseRows = 0;;
		Set<CharacteristicSet> denseCSs = new HashSet<CharacteristicSet>();
		for(CharacteristicSet nextCS : csMap.keySet()) {
			//if(immediateAncestors.containsKey(nextCS))
			//	continue;
//			int bb = Math.max(total/csMap.size()*meanMultiplier*2, total/100);
			
//			if(csSizes.get(nextCS) >= Math.max(total/csMap.size()*meanMultiplier*2, total/100)){ //-initial Marios Implementation
			if(csSizes.get(nextCS) >= maxCSSize*meanMultiplier/100){ //Dolap definition
			
				noOfdenseCS++;
				denseCSs.add(nextCS);
				totalDenseRows += csSizes.get(nextCS);
			}

		}
		System.out.println("Total CSs: " + csMap.size());
		System.out.println("Dense CSs: " + noOfdenseCS);
		//				for(NewCS nextDense : denseCSs){
		//					System.out.println("\t"+nextDense.toString());
		//				}
		System.out.println("Dense CS Coverage: " + totalDenseRows);						

		//System.out.println("\n\n\n\n");
		
		
		/**
		 * Create a path for each dense node from detected parent child relationships and assign costs  
		 */
		HashMap<Path, Integer> pathCosts = new HashMap<Path, Integer>();

		//findPaths returns a path for each dense node. 
		Set<Path> foundPaths = findPaths(denseCSs, pathCosts, csSizes, reverseImmediateAncestors, true, false);

//		clean up internal paths and keep only longest ones  
		for(Path outerPath : foundPaths){
			for(Path innerPath : foundPaths){
				if(outerPath.equals(innerPath)) continue;					
				if(innerPath.containsAll(outerPath)){
					foundPaths.remove(outerPath);
					break;
				}					
			}
		}	
				
		//				for(Path nextPath : foundPaths){
		//					System.out.println(pathCosts.get(nextPath)+": " + nextPath.toString()) ;
		//				}
		//System.out.println("\n\n\n\n");

		/**
		 * Merging paths into Dense Nodes and move merged CSs properties to the merged Dense nodes
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
		
		//sort paths based on cost DESCENDING  order. Cost of a path is the #triples contained in all CSs in path  
		List<Path> orderedPaths = new ArrayList<Path>(foundPaths);			
		Collections.sort(orderedPaths, new Comparator<Path>() {

			public int compare(Path o1, Path o2) {
				if (pathCosts.get(o1) > pathCosts.get(o2)) return -11; //tell sorting that if o1>o2 then o1 should come before o2
				else if (pathCosts.get(o1) < pathCosts.get(o2)) return 1; //tell sorting that if o1<o2 then o1 should come after o2
				else return 0;

			}
		});
		
		//create final list of paths and update the costs of paths , i.e., cardinality
		List<Path> finalList = new ArrayList<Path>();		
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
				updateCardinality(smallPath, csSizes, pathCosts);	
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

		Iterator<Path> finalIt = orderedPaths.iterator();
		while(finalIt.hasNext()){
			Path n = finalIt.next();
			if(n.isEmpty()){
				finalIt.remove();
				pathCosts.remove(n);
			}
			else{
				updateCardinality(n, csSizes, pathCosts);
				finalList.add(n);
			}
		}
		Set<CharacteristicSet> finalUnique = new HashSet<CharacteristicSet>();
		for(Path finalCS : finalList){

			finalUnique.addAll(finalCS);
		}
		//System.out.println("\n\n\n\n\n");
		//				for(List<NewCS> nextPath : finalList){
		//					System.out.println(pathCosts.get(nextPath)+": " + nextPath.toString()) ;
		//				}
		System.out.println(finalUnique.size());
		System.out.println(finalList.size() + ": " + finalList.toString());

		/**
		 * Process CS not contained or covered in paths  
		 */
		int notCovered = 0;
		Set<CharacteristicSet> notContained = new HashSet<CharacteristicSet>();
		for(CharacteristicSet nextCS : csMap.keySet()){
			if(!finalUnique.contains(nextCS)){
				//System.out.println("Not contained: ("+csSizes.get(nextCS)+")" + nextCS.toString()) ;
				notCovered += csSizes.get(nextCS);
				notContained.add(nextCS);
			}
		}
		System.out.println("Not covered : " + notCovered); 

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
						Set<CharacteristicSet> children = ancestors.getOrDefault(parent, new HashSet<CharacteristicSet>());
						children.add(child);
						remainingAncestors.put(parent, children);
					}
				}

			}

		}

		System.out.println("Remaining ancestor listing complete.");
		Map<CharacteristicSet, Set<CharacteristicSet>> remainingImmediateAncestors = getImmediateAncestors(remainingAncestors);
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
			//if(immediateAncestors.containsKey(nextCS))
			//	continue;
			if(csSizes.get(nextCS) >= Math.max((notCovered)/notContained.size()*meanMultiplier, total/1000)){
				//denseCS++;
				remainingDenseCSs.add(nextCS);
				//totalDenseRows += csSizes.get(nextCS);
			}

		}
		Set<Path> remainingPaths = findPaths(remainingDenseCSs, remainingCosts, csSizes, reverseImmediateAncestorsNotContained, true, true);

		int totalCovered = 0;

		for(Path nextPath : finalList){

			//System.out.println("next path: " + nextPath );
			totalCovered += pathCosts.get(nextPath);

		}
		System.out.println("Total coverage: " + totalCovered) ;

		int totalRemaining = 0;

		for(Path nextPath : remainingPaths){

			//System.out.println("next path: " + nextPath );
			totalRemaining  += remainingCosts.get(nextPath);

		}
		System.out.println("Total remaining:" + totalRemaining) ;
		System.out.println("Size of remaining Paths: " + remainingPaths.size());

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

		System.out.println("Removed contained.");
		//				for(Path nextPath : foundPaths){
		//					System.out.println(pathCosts.get(nextPath)+": " + nextPath.toString()) ;
		//				}
		//System.out.println("\n\n\n\n");

		System.out.println("Sorting...");
		List<Path> remainingOrderedPaths = new ArrayList<Path>(remainingPaths);			
		Collections.sort(remainingOrderedPaths, new Comparator<Path>() {

			public int compare(Path o1, Path o2) {
				if (remainingCosts.get(o1) > remainingCosts.get(o2)) return -11;
				else if (remainingCosts.get(o1) < remainingCosts.get(o2)) return 1;
				else return 0;

			}
		});
		System.out.println("Done.");
		//				for(Path nextPath : orderedPaths){
		//					System.out.println(pathCosts.get(nextPath)+": " + nextPath.toString()) ;
		//				}
		List<Path> remainingFinalList = new ArrayList<Path>();

		totalIterations = 0;					

		System.out.println("Pruning...");
		for(int i = 0; i < remainingOrderedPaths.size(); i++){

			bigPath = remainingOrderedPaths.get(i);

			for(int k = i+1; k < remainingOrderedPaths.size(); k++){

				Path nextCS = remainingOrderedPaths.get(k);

				nextCS.removeAll(bigPath);

				updateCardinality(nextCS, csSizes, remainingCosts);	

			}				

			Collections.sort(remainingOrderedPaths.subList(i+1, remainingOrderedPaths.size()), new Comparator<Path>() {

				public int compare(Path o1, Path o2) {
					if (remainingCosts.get(o1) > remainingCosts.get(o2)) return -1;
					else if (remainingCosts.get(o1) < remainingCosts.get(o2)) return 1;
					else return 0;

				}
			});

		}
		System.out.println("Done.");
		finalIt = remainingOrderedPaths.iterator();
		while(finalIt.hasNext()){
			Path n = finalIt.next();
			if(n.isEmpty()){
				finalIt.remove();
				remainingCosts.remove(n);
			}
			else{
				updateCardinality(n, csSizes, remainingCosts);
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
		System.out.println("Total remaining:" + totalRemaining) ;
		System.out.println("Remaining Unique: " + remainingFinalUnique.size());
		//end remaining cleanup

		int coveredSoFar = totalCovered + totalRemaining;

		System.out.println("Dataset coverage: "  +  ((double)coveredSoFar/(double)total));

		Map<Path, Integer> pathMap = new HashMap<Path, Integer>();
		int pathIndex = 0;
		
		//contains all paths. Maps a path id to a list of triples 
		Map<Integer, int[][]> mergedMapFull = new HashMap<Integer, int[][]>();
		Map<CharacteristicSet, Path> csToPathMap = new HashMap<CharacteristicSet, Path>();
		finalList.addAll(remainingFinalList);

		Set<CharacteristicSet> finalNotCovered = new HashSet<CharacteristicSet>();

		Path finalNotCoveredList = new Path();

		for(CharacteristicSet abandoned : csMap.keySet()){
			if(finalUnique.contains(abandoned) || remainingFinalUnique.contains(abandoned)){
				continue;
			}
			else{
				finalNotCovered.add(abandoned);
				finalNotCoveredList.add(abandoned);
			}
		}
		//also compute new density factor
		int totalCSInPaths = 0;
		for(Path pathP : finalList){
			pathMap.put(pathP, pathIndex++);
			int[][] concat= csMapFull.get(csMap.get(pathP.get(0)));
//			int[][] concat = triples;
			csToPathMap.put(pathP.get(0), pathP);
			totalCSInPaths += pathP.size();
			for(int i = 1; i < pathP.size(); i++){
				concat = ArrayUtils.addAll(concat, csMapFull.get(csMap.get(pathP.get(i)))) ;
				csToPathMap.put(pathP.get(i), pathP);						
			}
			mergedMapFull.put(pathMap.get(pathP), concat);

		}
		//now for the abandoned
		if (finalNotCoveredList.size()>0) {
				pathMap.put(finalNotCoveredList, pathIndex++);
		int[][] triples = csMapFull.get(csMap.get(finalNotCoveredList.get(0)));
		int[][] concat = triples;
		csToPathMap.put(finalNotCoveredList.get(0), finalNotCoveredList);				
		for(int i = 1; i < finalNotCoveredList.size(); i++){
			concat = ArrayUtils.addAll(concat, csMapFull.get(csMap.get(finalNotCoveredList.get(i)))) ;
			csToPathMap.put(finalNotCoveredList.get(i), finalNotCoveredList);						
		}
		mergedMapFull.put(pathMap.get(finalNotCoveredList), concat);
		//end abandoned
		}
		
		double density = (double) coveredSoFar / totalCSInPaths ;  
		System.out.println("Density: " + density);
		Map<Integer, Path> reversePathMap = new HashMap<Integer, Path>();
		for(Path pathP : pathMap.keySet())
			reversePathMap.put(pathMap.get(pathP), pathP);
		//if(true) return ;
		System.out.println("merged map full: " + mergedMapFull.toString());
		Iterator<Map.Entry<Integer, int[][]>> tripleGroups = mergedMapFull.entrySet().iterator();


		/**
		 * Create database tables
		 *    
		 */
		
		/**
		 * Creation of a dictionary table with a row for each distinct subject and object o  
		 */

		StringBuilder sb2 = new StringBuilder();
		CopyManager cpManager2;
		System.out.println("Adding keys to dictionary. " + new Date().toString());
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS dictionary (id INT, label INT); ");
			stmt.close();		       


			cpManager2 = ((PGConnection)conn).getCopyAPI();
			PushbackReader reader2 = new PushbackReader( new StringReader(""), 20000 );
			Iterator<Map.Entry<String, Integer>> keyIt = intMap.entrySet().iterator();
			int iter = 0;
			while(keyIt.hasNext())
			{
				Entry<String, Integer> nextEntry = keyIt.next();
				sb2.append(nextEntry.getValue()).append(",")		      
				.append(nextEntry.getKey().hashCode()).append("\n");
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
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Done adding keys to dictionary. " + new Date().toString());
		
		
		/**
		 * Create database table cs schema. CS schema contains the id of a CS and the array of properties (int[])
		 */
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
				
				try{				
					//c.setAutoCommit(false);
					stmt = conn.createStatement();
					stmt.executeUpdate(createTableQuery.toString());
					stmt.close();		       

				} catch (Exception e){
					e.printStackTrace();
					return ;
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
						return ;
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
						//System.out.println("1"  + rucs.get(dbECSMap.get(tripleNext[2])));
						//System.out.println("2" + csToPathMap.get(rucs.get(dbECSMap.get(tripleNext[2]))));
						if(csToPathMap.containsKey(reverseCSMap.get(dbECSMap.get(tripleNext[2])))){
							int pairedPathIndex = pathMap.get(csToPathMap.get(reverseCSMap.get(dbECSMap.get(tripleNext[2]))));
							String pairString = ""+nextPathIndex +"_"+pairedPathIndex;
							pathPairs.add(pairString);
							Set<Integer> ecsProp = pathPairProperties.getOrDefault(pairString, new HashSet<Integer>());
							ecsProp.add(tripleNext[1]) ;
							pathPairProperties.put(pairString, ecsProp) ;																																			

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
		System.out.println("Ending time: " + new Date().toString());


	}

	/***
	 * 
	 * @param ancestors is the graph of all CS in the data 
	 * @return a collection that for a cs provides all direct ancestors (1-hop)    
	 */
	
	public static Map<CharacteristicSet, Set<CharacteristicSet>> getImmediateAncestors(Map<CharacteristicSet, Set<CharacteristicSet>> ancestors){
		
		Map<CharacteristicSet, Set<CharacteristicSet>> immediateAncestors = new HashMap<CharacteristicSet, Set<CharacteristicSet>>();
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
			immediateAncestors.put(parent,children ) ;


		}

		return immediateAncestors;
	}

	
	/***
	 * Takes as input a path and updates its cost based on the CS it contains.  
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
	 * 
	 * @param denseCSs are the dense cs
	 * @param pathCosts the cost of a path between two CSs. The cost of a path is the cardinality , i.e., the no of triples contained in ALL cs in the path 
	 * @param csSizes the no of triples a cs contains
	 * @param reverseImmediateAncestors a collection with mappings from a parent cs to its child
	 * @param denseCheck
	 * @param withSiblings
	 * @return foundPaths is a Set containing mapping between a dense CS and a CS in the ancestor graph for which a path exists!
	 */
	public static Set<Path> findPaths(Set<CharacteristicSet> denseCSs, 
			Map<Path, Integer> pathCosts, 
			Map<CharacteristicSet, Integer> csSizes, 
			Map<CharacteristicSet, Set<CharacteristicSet>> reverseImmediateAncestors, 
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
				if(reverseImmediateAncestors.get(curCS).isEmpty()){
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

				if(!reverseImmediateAncestors.get(curCS).isEmpty()){

					for(CharacteristicSet parent : reverseImmediateAncestors.get(curCS)){							
						if(denseCheck && denseCSs.contains(parent)) {

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

	}

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
}
