package com.athena.imis.querying.separatism;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.graph.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.athena.imis.models.DirectedGraph;
import com.athena.imis.querying.common.IRelationalQueryArray;
import com.athena.imis.querying.extras.SimpleClientQueryIS;
import com.athena.imis.models.CharacteristicSet;

public class RelationalQueryArrayIS20Separatism implements IRelationalQueryArray {

	private static final Logger LOG = LogManager.getLogger(SimpleClientQueryIS.class);

	public static Connection conn ;
	private String[] args;
	private double execTime , planTime ;
	private long time = 0;
	
	//variable for the query
	Map<CharacteristicSet, List<CharacteristicSet>> csJoinMap;
	Set<CharacteristicSet> csSet ;
	
	Map<CharacteristicSet, String> csVarMap = new HashMap<CharacteristicSet, String>();
	Map<CharacteristicSet, Set<String>> csMatches = new HashMap<CharacteristicSet, Set<String>>();
	
	private Statement st;
	
	
	//dictionary variables
	HashMap<String, Integer> propMap = new HashMap<String, Integer>();
	Map<CharacteristicSet, Set<Integer>> multiValuedCSProps = new HashMap<CharacteristicSet, Set<Integer>>();
	Map<Integer, CharacteristicSet> realCSIds = new HashMap<Integer, CharacteristicSet>();
	Set<String> pathSet = new HashSet<String>();
	
	//keeps the list of final sql queries
	List<String> FinalSqlQueries = new ArrayList<String>(); 

	
	public RelationalQueryArrayIS20Separatism(String[] args) {
		this.args = args;
		this.loadDictionary();
	}
	
	protected  <T> List<List<T>> cartesianProduct(List<List<T>> lists) {
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
	
	/***
	 * init dictionary db
	 */
	private void loadDictionary() {
		execTime = 0d; planTime = 0d;
		
		try{
			
			conn = this.getConnection();
			ResultSet rs;
			st = conn.createStatement();				
			
			String propertiesSetQuery = " SELECT id, uri FROM propertiesset ;";
			ResultSet rsProps = st.executeQuery(propertiesSetQuery);
			
			while(rsProps.next()){
				propMap.put(rsProps.getString(2), rsProps.getInt(1));
			}
			rsProps.close();
			st.close();
			
			st = conn.createStatement();				
			
			String multiValuedQuery = " SELECT cs, p, properties FROM multi_valued INNER JOIN cs_schema ON cs=id ;";
			ResultSet rsMulti = st.executeQuery(multiValuedQuery);
			while(rsMulti.next()){
				Array a = rsMulti.getArray(3);
				Integer[] arr = (Integer[])a.getArray();
				CharacteristicSet arrCS = new CharacteristicSet(arr);
				Set<Integer> thisCS = multiValuedCSProps.getOrDefault(arrCS, new HashSet<Integer>());
				
				thisCS.add(rsMulti.getInt(2));
				multiValuedCSProps.put(arrCS, thisCS) ;
				realCSIds.put(rsMulti.getInt(1), arrCS);
			}
			rsMulti.close();
			st.close();
			st = conn.createStatement();	
			String paths = "select DISTINCT e1.css, e2.css, e3.css, e4.css, e5.css, e6.css from ecs_schema as e1 "
					+ "inner join ecs_schema as e2 on e1.cso  = e2.css "
					+ "inner join ecs_schema as e3 on e2.cso  = e3.css "
					+ "inner join ecs_schema as e4 on e3.cso  = e4.css "
					+ "inner join ecs_schema as e5 on e4.cso  = e5.css "
					+ "inner join ecs_schema as e6 on e5.cso  = e6.css";
			ResultSet rsPaths = st.executeQuery(paths);
			while(rsPaths.next()){
				String pathList = "";
				for(int i = 1; i < 7; i++){
					pathList += rsPaths.getInt(i)+"_";
				}
				pathList = pathList.substring(0, pathList.length()-1);
				pathSet.add(pathList);
			}
			System.out.println("PathSet: " + pathSet.toString());
			rsPaths.close();
			st.close();
			
			System.out.println("propMap: " + propMap.toString()) ;
		}catch (Exception e){
			e.printStackTrace();
			return ;
		}
			
		
	}
	
	
	/* (non-Javadoc)
	 * @see com.athena.imis.querying.IRelationalQueryArray#generateSQLQuery(java.lang.String)
	 */
	
	@Override
	public String generateSQLQuery(String sparql) {
		String finalQuery = "";
		try{
			
			
			execTime = 0d;
			planTime = 0d;

			//initialize SPARQL extractor
			QueryParserIS20Separatism sparqlParser = new QueryParserIS20Separatism(conn);
			sparqlParser.setSparql(sparql);
			sparqlParser.setPropertyMap(propMap);
			sparqlParser.parseSPARQL();

			//extract CSs and ECS
			csJoinMap = sparqlParser.getCsJoinMap();
			csSet = sparqlParser.getCsSet();

			
			///map cs to data 
			this.mapCS();

			/***
			 * Build SQL syntax
			 */

			//prepare WHERE clause
			String sqlExpression = "";
			String where = " WHERE ";
			StringBuilder unionClause = new StringBuilder();
			
			//sqlTranslator.getObjectMap() ;
			//List<String> resList = new ArrayList<String>();

			Map<CharacteristicSet, List<Triple>> csRestrictions = sparqlParser.getCsRestrictions();

			//generate aliases for CSs
			Map<CharacteristicSet, String> csAliases = new HashMap<CharacteristicSet, String>();

			int csAliasIdx = 0; for(CharacteristicSet nextCS : csMatches.keySet()){csAliases.put(nextCS, "c"+(csAliasIdx++));}

			Map<String, String> csIdAliases = new HashMap<String, String>();

			csAliasIdx = 0; 
			for(CharacteristicSet nextCS : csMatches.keySet()){
				for(String nextCSId : nextCS.getMatches())
					csIdAliases.put(nextCSId, "c"+(csAliasIdx));
				csAliasIdx++;
			}

			for(CharacteristicSet nextCS : csMatches.keySet()){

				Set<Integer> isCovered = new HashSet<Integer>();
				if(csRestrictions.containsKey(nextCS)){

					List<Triple> restrictions = csRestrictions.get(nextCS);

					for(Triple nextRes : restrictions){

						String restriction = "NULL";

						//we need the CS alias here
						if(nextRes.getObject().isURI()){
							restriction = csAliases.get(nextCS) + ".p_" + 
									propMap.get("<" + nextRes.getPredicate().toString()+">") + " = " + 
									sparqlParser.getObjectMap().get("<" + nextRes.getObject().toString()+">");
						}
						else if(nextRes.getObject().isLiteral()){
							restriction = csAliases.get(nextCS) + ".p_" + 
									propMap.get("<" + nextRes.getPredicate().toString()+">") + " = " + 
									sparqlParser.getObjectMap().get(nextRes.getObject().toString());
						}

						isCovered.add(propMap.get("<" + nextRes.getPredicate().toString()+">"));

						if(!where.equals(" WHERE "))
							where += " AND ";
						where += restriction ;

					}
				}
				else if(sparqlParser.getSubjectMap().containsKey(nextCS)){
					String restriction = csAliases.get(nextCS) + ".s = " + 
							sparqlParser.getSubjectMap().get(nextCS);

					if(!where.equals(" WHERE "))
						where += " AND ";

					where += restriction ;
				}
				//else {

				//check for all nulls??
				String nonNull = "";
				// All properties contained in the triple graph (i.e.contained in a query CS)  must be not null, 
				// either they are part of a restriction or a join
				for(Integer property : nextCS.getAsList()){
					if(isCovered.contains(property)) continue; 
					nonNull = csAliases.get(nextCS) + ".p_" +property+" IS NOT NULL ";

					if(!where.equals(" WHERE "))
						where += " AND ";
					where += nonNull ;
				}

				continue;
				//}
				
			}
		
			//System.out.println(where);

			//find permutations of cs
			List<List<String>> csAsList = new ArrayList<List<String>>();
			List<CharacteristicSet> csMatchesOrdered = new ArrayList<CharacteristicSet>(csMatches.keySet());

			Map<CharacteristicSet, Integer> csListIndexMap = new HashMap<CharacteristicSet, Integer>();

			int csIndexMap = 0;
			for(CharacteristicSet nextCS : csMatchesOrdered){
				csAsList.add(new ArrayList<String>(nextCS.getMatches()));
				csListIndexMap.put(nextCS, csIndexMap++);
			}
			//System.out.println("CS list index map: " + csListIndexMap.toString());
			List<List<String>> perms = cartesianProduct(csAsList);
		
		
			//Build the projection part , i.e., SELECT Clause....

			Map<CharacteristicSet, List<Triple>> vars = sparqlParser.getCsVars();
			//System.out.println("vars " + vars.toString()) ;
			List<String> csProjectionsOrdered = new ArrayList<String>();
			//System.out.println("CS MATCHES ORDERED " + csMatchesOrdered.toString());
			for(CharacteristicSet nextCS : csMatchesOrdered){

				//System.out.println("NEXT CS: " + nextCS.toString());
				//System.out.println("CS PROJECTIONS : " + csProjectionsOrdered.toString());
				String csProjection = "";
				csProjection += csAliases.get(nextCS) + ".s";
				//System.out.println("csProjection: " + csProjection);
				csProjectionsOrdered.add(csProjection);
				csProjection = "";
				//			if(!vars.containsKey(nextCS)) {
				//				csProjection += csAliases.get(nextCS) + ".s";
				//				csProjectionsOrdered.add(csProjection);
				//				continue;
				//			}
				if(!vars.containsKey(nextCS)) {continue;}
				List<Triple> nextCSVars = vars.get(nextCS);
				if(!csProjection.equals(""))
					csProjection += ", ";
				//System.out.println(nextCSVars.toString());

				for(Triple t : nextCSVars){
					csProjection += csAliases.get(nextCS) + ".p_"+propMap.get("<" + t.getPredicate().toString()+">") + ", ";
				}
				csProjection = csProjection.substring(0, csProjection.length()-2);
				csProjectionsOrdered.add(csProjection);
			}


			//Build the FROM Part

			//are there any joins? 
 
			boolean joinsExist = false;
			for(CharacteristicSet nextCSS : csJoinMap.keySet()){
				if(csJoinMap.get(nextCSS) != null){
					joinsExist = true;
					break;
				}
			}

			//get the query graph representation?
			DirectedGraph<CharacteristicSet> queryGraph = sparqlParser.getQueryGraph();

			/**
			 * If the query has no joins then only a single query CS exists and it may be mapped to multiple data CSs (permutations)
			 * 	For each permutation a SQL is created 
			 *  All permutations are concatenated with UNION  
			 */
			if(!joinsExist){
				//one query with no joins for each permutation
				for(List<String> nextPerm : perms){

					int idx = 0;

					List<Integer> asIntList = new ArrayList<Integer>();
					for(String nextCS : nextPerm){
						asIntList.add(Integer.parseInt(nextCS));
					}
					idx = 0;
					for(Integer nextCS : asIntList){

						String varList = csProjectionsOrdered.get(idx++);
						if(!sqlExpression.equals("")){
							sqlExpression += " UNION ";			
						}
						if(!where.equals(" WHERE "))
							sqlExpression += " (SELECT " + varList + " FROM cs_" + nextCS + " AS " + csIdAliases.get(nextCS+"")+ " " + where + ") ";
						else
							sqlExpression += " (SELECT " + varList + " FROM cs_" + nextCS + " AS " + csIdAliases.get(nextCS+"")+ ") ";
					}
					idx = 0;
					

					//since it is single table (no joins) replace c_* pattern with the index of each permutation and build the query
					for(String nextIntString : nextPerm) {
						CharacteristicSet nextCSToTransform = csMatchesOrdered.get(idx++);
						sqlExpression = sqlExpression.replaceAll("cs_ AS "+csAliases.get(nextCSToTransform), "cs_"+nextIntString+" AS "+csAliases.get(nextCSToTransform));

						//regexes for multivalued properties

						//pattern for literal values
						String patternString1 = "("+csAliases.get(nextCSToTransform)+".p_[0-9]* = [0-9][0-9]*)";							
						Pattern pattern = Pattern.compile(patternString1);
						Matcher matcher = pattern.matcher(sqlExpression);
						int nextInt = Integer.parseInt(nextIntString);
						while(matcher.find()) {
							//System.out.println("found: " + matcher.group(1));
							String nextProperty =  matcher.group(1);//matcher.group(1).replaceAll(csAliases.get(nextCSToTransform)+".p_","");
							Pattern patternProp = Pattern.compile(".p_([0-9]*)");
							Matcher matcherProp = patternProp.matcher(nextProperty);
							int intProp ;
							if(matcherProp.find()){
								intProp = Integer.parseInt(matcherProp.group(1));
								if(multiValuedCSProps.containsKey(realCSIds.get(nextInt))
										&& multiValuedCSProps.get(realCSIds.get(nextInt)).contains(intProp)){

									//System.out.println("is contained in multivar");

									String restr = matcher.group(1);
									//System.out.println("matcher group 1: " + restr);
									restr = restr.split(" = ")[1] + " = ANY("+restr.split(" = ")[0]+")";
									//restr += "] ";
									sqlExpression = sqlExpression.replaceAll(matcher.group(1), restr);//matcher.replaceFirst(restr);        		
									//System.out.println("replaced: " + templateQ);
								}
							}
							//System.out.println(realCSIds.get(nextInt));


						}

						//pattern for .s equalities
						String patternString2 = "("+csAliases.get(nextCSToTransform)+".p_[0-9]* = c[0-9]*.s)";							
						Pattern pattern2 = Pattern.compile(patternString2);
						Matcher matcher2 = pattern2.matcher(sqlExpression);
						nextInt = Integer.parseInt(nextIntString);
						while(matcher2.find()) {
							//System.out.println("found: " + matcher.group(1));
							String nextProperty =  matcher2.group(1);//matcher.group(1).replaceAll(csAliases.get(nextCSToTransform)+".p_","");
							Pattern patternProp = Pattern.compile(".p_([0-9]*)");
							Matcher matcherProp = patternProp.matcher(nextProperty);
							int intProp ;
							if(matcherProp.find()){
								intProp = Integer.parseInt(matcherProp.group(1));
								if(multiValuedCSProps.containsKey(realCSIds.get(nextInt))
										&& multiValuedCSProps.get(realCSIds.get(nextInt)).contains(intProp)){

									String restr = matcher2.group(1);
									//System.out.println("matcher group 1: " + templateQ);
									String[] split = restr.split(" = ");
									restr = split[1] + " = ANY("+split[0]+")";
									sqlExpression = sqlExpression.replaceAll(matcher2.group(1), restr);//matcher.replaceFirst(restr);   
									sqlExpression = sqlExpression.replaceAll("AND "+split[0]+" IS NOT NULL", "");
									sqlExpression = sqlExpression.replaceAll(""+split[0]+" IS NOT NULL", "");
									//System.out.println("replaced: " + templateQ);
								}
							}
						}
					}
				}	

				finalQuery = sqlExpression; 
				
			}
		
		
			/**
			 * If the query has joins then 
			 */

			else{

				Set<CharacteristicSet> graphRoots = queryGraph.findRoots() ;

				if(graphRoots.isEmpty()) // no roots, must be a cyclic query -- let's iterate through every node!
					graphRoots = queryGraph.getmGraph().keySet();

				Stack<CharacteristicSet> stack = new Stack<CharacteristicSet>();								

				Map<CharacteristicSet, LinkedHashSet<CharacteristicSet>> joinQueues = new HashMap<CharacteristicSet, LinkedHashSet<CharacteristicSet>>();

				for(CharacteristicSet root : graphRoots){

					stack.push(root);

					Set<CharacteristicSet> visited = new HashSet<CharacteristicSet>();

					LinkedHashSet<CharacteristicSet> currentQueue = joinQueues.getOrDefault(root, new LinkedHashSet<CharacteristicSet>()) ;

					while(!stack.isEmpty()){

						CharacteristicSet currentCS = stack.pop();												

						//System.out.println("next popped : " + currentCS.toString());

						visited.add(currentCS);

						if(queryGraph.isSink(currentCS))
							continue;

						currentQueue.add(currentCS);

						for(CharacteristicSet child : queryGraph.edgesFrom(currentCS)){

							//if(!visited.contains(child)){

							stack.push(child);

							if(joinQueues.containsKey(child)){
								currentQueue = joinQueues.get(child) ;
								currentQueue.add(currentCS) ;
								joinQueues.remove(currentCS);
								joinQueues.remove(child);
								joinQueues.put(currentCS, currentQueue);
								joinQueues.put(child, currentQueue);
							}
							else{
								currentQueue.add(child);
								joinQueues.remove(currentCS);
								joinQueues.put(currentCS, currentQueue);
								joinQueues.put(child, currentQueue);
							}
							//}

						}


					}
				}
				//System.out.println("\n");
				//System.out.println(joinQueues.toString());
				Set<LinkedHashSet<CharacteristicSet>> uniqueQueues = new HashSet<LinkedHashSet<CharacteristicSet>>();
				for(CharacteristicSet nextCS : joinQueues.keySet()){
					uniqueQueues.add(joinQueues.get(nextCS));
				}					



				String varList = "";
				//System.out.println(" projections " + csProjectionsOrdered.toString()) ;
				for(int ig = 0; ig < csProjectionsOrdered.size(); ig++){
					varList += csProjectionsOrdered.get(ig) + ", ";
				}
				varList = varList.substring(0, varList.length()-2) ;
				Map<CharacteristicSet, Set<CharacteristicSet>> reverseJoinMap = new HashMap<CharacteristicSet, Set<CharacteristicSet>>();

				//reverse the join map...
				for(CharacteristicSet key : csJoinMap.keySet()){
					if(!csJoinMap.containsKey(key) || csJoinMap.get(key) == null) continue;
					for(CharacteristicSet nextValue : csJoinMap.get(key)){

						Set<CharacteristicSet> values = reverseJoinMap.getOrDefault(nextValue, new HashSet<CharacteristicSet>());
						values.add(key) ;
						reverseJoinMap.put(nextValue, values) ;				
					}
				}

				for(LinkedHashSet<CharacteristicSet> nextQueue : uniqueQueues){

					List<CharacteristicSet> qAsList = new ArrayList<CharacteristicSet>(nextQueue) ;
					int jid = 0;
					sqlExpression = "";

					HashSet<CharacteristicSet> visited = new HashSet<CharacteristicSet>();

					for(int i = 0; i < qAsList.size(); i++){

						CharacteristicSet nextCS = qAsList.get(i);
						//System.out.println("next CS: " + nextCS + " alias " + csAliases.get(nextCS));
						visited.add(nextCS) ;

						//NewCS toJoinCS = qAsList.get(i+1);

						if(jid++ == 0){
							sqlExpression = " SELECT " + varList + " FROM cs_ AS " +csAliases.get(nextCS) + " " ;  
						}
						else {
							if(reverseJoinMap.get(nextCS) != null) {
								//System.out.println("here!") ;
								sqlExpression += " INNER JOIN cs_ AS " + csAliases.get(nextCS) + " ON " ;

								for(CharacteristicSet nextReverseJoin : reverseJoinMap.get(nextCS)){
									if(visited.contains(nextReverseJoin)){
										List<CharacteristicSet> joinKey = new ArrayList<CharacteristicSet>();

										joinKey.add(nextReverseJoin);

										joinKey.add(nextCS);

										//List<Triple> joinProps = sqlTranslator.getCsJoinProperties().get(joinKey);

										List<Integer> joinProps = new ArrayList<Integer>();
										//System.out.println("joinKey: " + joinKey.toString());
										for(Triple nextJoinTriple : sparqlParser.getCsJoinProperties().get(joinKey)){

											joinProps.add(propMap.get("<" + nextJoinTriple.getPredicate().toString()+">"));
										}
										for(int j = 0; j < joinProps.size(); j++){
											sqlExpression += csAliases.get(nextReverseJoin)+".p_"+joinProps.get(j) + " = " + csAliases.get(nextCS)+".s AND ";												
										}	
									}										
								}
								sqlExpression = sqlExpression.substring(0, sqlExpression.length()-4);
							}

							else if(csJoinMap.get(nextCS) != null) {
								//System.out.println("here2!") ;
								//if(!visited.contains(nextCS))
								sqlExpression += " INNER JOIN cs_ AS " + csAliases.get(nextCS) + " ON " ;

								for(CharacteristicSet nextReverseJoin : csJoinMap.get(nextCS)){
									if(visited.contains(nextReverseJoin)){
										List<CharacteristicSet> joinKey = new ArrayList<CharacteristicSet>();

										joinKey.add(nextCS);

										joinKey.add(nextReverseJoin);

										//List<Triple> joinProps = sqlTranslator.getCsJoinProperties().get(joinKey);

										List<Integer> joinProps = new ArrayList<Integer>();

										for(Triple nextJoinTriple : sparqlParser.getCsJoinProperties().get(joinKey)){

											joinProps.add(propMap.get("<" + nextJoinTriple.getPredicate().toString()+">"));

										}

										for(int j = 0; j < joinProps.size(); j++){	

											sqlExpression += csAliases.get(nextCS)+".p_"+joinProps.get(j) + " = " + csAliases.get(nextReverseJoin)+".s AND ";												

										}

									}										
								}
								sqlExpression = sqlExpression.substring(0, sqlExpression.length()-4);
							}

						}
					}
					
					//replace for each join queue the aliases for the CS involved 
					System.out.println("Number of permutations: " + perms.size());
					for(List<String> nextPerm : perms){
						String nextPermInt = "";
						for(String nextPermCS : nextPerm){
							nextPermInt += nextPermCS+"_";
						}
						nextPermInt = nextPermInt.substring(0, nextPermInt.length()-1);
						boolean isContained = false;
						for(String nextPathList : pathSet){
							if(nextPathList.contains((nextPermInt))){
								isContained = true;
								break;
							}
						}
						String sqlTemp =  sqlExpression;
						if(!where.equals(" WHERE "))
							sqlTemp += where ;	
						int idx = 0;
						//System.out.println("nextPerm: " + nextPerm.toString()) ;						
						for(String nextIntString : nextPerm) {
							CharacteristicSet nextCSToTransform = csMatchesOrdered.get(idx++);
							sqlTemp = sqlTemp.replaceAll("cs_ AS "+csAliases.get(nextCSToTransform), "cs_"+nextIntString+" AS "+csAliases.get(nextCSToTransform));

							//regexes for multivalued properties

							//pattern for literal values
							String patternString1 = "("+csAliases.get(nextCSToTransform)+".p_[0-9]* = [0-9][0-9]*)";							
							Pattern pattern = Pattern.compile(patternString1);
							Matcher matcher = pattern.matcher(sqlTemp);
							int nextInt = Integer.parseInt(nextIntString);
							while(matcher.find()) {
								//System.out.println("found: " + matcher.group(1));
								String nextProperty =  matcher.group(1);//matcher.group(1).replaceAll(csAliases.get(nextCSToTransform)+".p_","");
								Pattern patternProp = Pattern.compile(".p_([0-9]*)");
								Matcher matcherProp = patternProp.matcher(nextProperty);
								int intProp ;
								if(matcherProp.find()){
									intProp = Integer.parseInt(matcherProp.group(1));
									//System.out.println("found222: " + matcher.group(1));
									if(multiValuedCSProps.containsKey(realCSIds.get(nextInt))
											&& multiValuedCSProps.get(realCSIds.get(nextInt)).contains(intProp)){

										//System.out.println("is contained in multivar");

										String restr = matcher.group(1);
										//System.out.println("matcher group 1: " + restr);
										restr = restr.split(" = ")[1] + " = ANY("+restr.split(" = ")[0]+")";
										//restr += "] ";
										sqlTemp = sqlTemp.replaceAll(matcher.group(1), restr);//matcher.replaceFirst(restr);        		
										//System.out.println("replaced: " + templateQ);
									}
								}
								//System.out.println(realCSIds.get(nextInt));
							}
				        
							//pattern for .s equalities
							String patternString2 = "("+csAliases.get(nextCSToTransform)+".p_[0-9]* = c[0-9]*.s)";							
							Pattern pattern2 = Pattern.compile(patternString2);
							Matcher matcher2 = pattern2.matcher(sqlTemp);
							nextInt = Integer.parseInt(nextIntString);
							while(matcher2.find()) {
								//System.out.println("found: " + matcher.group(1));
								String nextProperty =  matcher2.group(1);//matcher.group(1).replaceAll(csAliases.get(nextCSToTransform)+".p_","");
								Pattern patternProp = Pattern.compile(".p_([0-9]*)");
								Matcher matcherProp = patternProp.matcher(nextProperty);
								int intProp ;
								if(matcherProp.find()){
									intProp = Integer.parseInt(matcherProp.group(1));
									if(multiValuedCSProps.containsKey(realCSIds.get(nextInt))
											&& multiValuedCSProps.get(realCSIds.get(nextInt)).contains(intProp)){

										String restr = matcher2.group(1);
										//System.out.println("matcher group 1: " + templateQ);
										String[] split = restr.split(" = ");
										restr = split[1] + " = ANY("+split[0]+")";
										sqlTemp = sqlTemp.replaceAll(matcher2.group(1), restr);//matcher.replaceFirst(restr);   
										sqlTemp = sqlTemp.replaceAll("AND "+split[0]+" IS NOT NULL", "");
										sqlTemp = sqlTemp.replaceAll(""+split[0]+" IS NOT NULL", "");
										//System.out.println("replaced: " + templateQ);
									}
								}


							}

						}

						unionClause.append(sqlTemp).append(" UNION ");
//						//if(true) continue ;
//						try{
//
//							Statement st2 = conn.createStatement();
//							String explain = "EXPLAIN ANALYZE " ;
//							//explain = "" ;
//							if(!where.equals(" WHERE ")){
//								sqlExpression = explain + sqlExpression ;//+ " " + where;	
//							}
//							else{
//								sqlExpression = explain + sqlExpression + " ";
//							}
//							System.out.println("\t" + sqlExpression);
//							//templateQ = templateQ.replaceAll("p_0 = 20", "p_0 = 22");
//							ResultSet rs2 = st2.executeQuery(sqlExpression); //
//							long start = System.nanoTime();
//							while (rs2.next())
//							{				    	
//								//System.out.println(rs2.getString(1));
//								if(rs2.getString(1).contains("Execution time: ")){
//									String exec = rs2.getString(1).replaceAll("Execution time: ", "").replaceAll("ms", "").trim();
//									execTime += Double.parseDouble(exec);
//
//								}
//								else if(rs2.getString(1).contains("Planning time: ")){
//									String plan = rs2.getString(1).replaceAll("Planning time: ", "").replaceAll("ms", "").trim();
//									execTime += Double.parseDouble(plan);
//									planTime += Double.parseDouble(plan);
//
//								}					   
//								res++;
//							}
//							rs2.close();
//							//System.out.println(execTime);
//							time += System.nanoTime() - start;
//							System.out.println(res);
//						
//						}
//						catch(SQLException e){
//							e.printStackTrace();
//						}
					}

				}												
				//remove any unnecessarily UNION added at the end.
				if(unionClause.length() >= 8)
					unionClause.delete(unionClause.length()-7, unionClause.length());
				
				finalQuery = unionClause.toString();
			}		
			
		}catch (Exception e){
			e.printStackTrace();
		}
		
		return finalQuery;
	}
	
	
	
	/***
	 * Map CS from query to Data CS via the ecs index.
	 * It implement lookup of the ECS_schema and fetches first CSs that have a subject - object join, i.e. CSS and CSO columns
	 * Next it iterates over the ECS_schema.css column which contains all CSs for getting any other CS that is not matched  
	 */
	private void mapCS() {
		
		//initialize maps 
		Set<CharacteristicSet> undangled = new HashSet<CharacteristicSet>();
		csVarMap = new HashMap<CharacteristicSet, String>();
		csMatches = new HashMap<CharacteristicSet, Set<String>>();
		Map<CharacteristicSet, Set<String>> csQueryMatches = new HashMap<CharacteristicSet, Set<String>>();

		try {
			//get ecs from db for each pair of SO joins
			for(CharacteristicSet nextCSS : csJoinMap.keySet()){

				if(csJoinMap.get(nextCSS) == null) 
					continue;
				for(CharacteristicSet nextCSO : csJoinMap.get(nextCSS)){

					//undangled.add(nextCSO);

					String schema = " SELECT DISTINCT * FROM ecs_schema as e "
							+ "WHERE e.css_properties @> ARRAY" + nextCSS.getAsList().toString() 
							+ " AND e.cso_properties @> ARRAY" + nextCSO.getAsList().toString();							

					st = conn.createStatement();
					ResultSet rsS = st.executeQuery(schema);
					
					while(rsS.next()){
						Set<String> css_matches = csQueryMatches.getOrDefault(nextCSS, new HashSet<String>());
						css_matches.add(rsS.getString(2));
						//if(csMatches.containsKey(nextCSS))
						csQueryMatches.put(nextCSS, css_matches);

						Set<String> cso_matches = csQueryMatches.getOrDefault(nextCSO, new HashSet<String>());
						cso_matches.add(rsS.getString(3));
						csQueryMatches.put(nextCSO, cso_matches);
					}
					rsS.close();
					st.close();
					//System.out.println("new round " + csMatches.toString());
					for(CharacteristicSet nextCS : csQueryMatches.keySet()){
						//System.out.println("next cs " + nextCS.toString());
						if(!csMatches.containsKey(nextCS)){
							//System.out.println("not contained");
							csMatches.put(nextCS, csQueryMatches.get(nextCS));
							//System.out.println("cs matches thus far: "+ csMatches.toString());
						}
						else{
							//System.out.println("contained");
							Set<String> c = csQueryMatches.get(nextCS);
							//System.out.println("existing matches: "+ csMatches.toString());
							//System.out.println("existing c: "+ c.toString());
							c.retainAll(csMatches.get(nextCS));
							//System.out.println("after retain: "+ c.toString());
							csMatches.put(nextCS, c) ;
						}

					}
					csQueryMatches.clear();
				}				

			}	 	
			
			for(CharacteristicSet nextCSS : csSet){

				if(csJoinMap.get(nextCSS) != null || undangled.contains(nextCSS))
					//nextCSS is already processed or matched
					continue;
				String schema = " SELECT DISTINCT * FROM ecs_schema as e "
						+ "WHERE e.css_properties @> ARRAY" + nextCSS.toString() ;

				// schema = " SELECT DISTINCT * FROM cs_schema as e "+ "WHERE e.properties @> ARRAY" + nextCSS.toString() ;			

				st = conn.createStatement();
				ResultSet rsS = st.executeQuery(schema);

				while(rsS.next()){
					Set<String> css_matches = csMatches.getOrDefault(nextCSS, new HashSet<String>());
					css_matches.add(rsS.getString(2));
					csMatches.put(nextCSS, css_matches);					
				}
				rsS.close();
				st.close();
			}

			for(CharacteristicSet nextCS : csMatches.keySet()){
				nextCS.setMatches(csMatches.get(nextCS));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
	
	
	
	/*** 
	 * gets connection to the db
	 * 
	 * @return a connection
	 * @throws SQLException
	 */
	private Connection getConnection() throws SQLException {

		try{

			Class.forName("org.postgresql.Driver");
			conn = DriverManager
					.getConnection("jdbc:postgresql://"+args[0]+":5432/" + args[1].toLowerCase(), args[2], args[3]);
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			System.exit(0);
		}	

		return conn;
	}
	
	
}
