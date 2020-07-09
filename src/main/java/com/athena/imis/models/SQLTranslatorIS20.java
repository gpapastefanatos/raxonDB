package com.athena.imis.models;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementPathBlock;


public class SQLTranslatorIS20 {

	Connection conn;
	
	String sparql ;
	
	DirectedGraph<CharacteristicSet> queryGraph ;
		
	Map<CharacteristicSet, List<CharacteristicSet>> csJoinMap ;
	
	Set<CharacteristicSet> csSet = new HashSet<CharacteristicSet>();
	
	Map<CharacteristicSet, List<Triple>> csRestrictions = new HashMap<CharacteristicSet, List<Triple>>();
	
	Map<String, Integer> objectMap ;
	
	Map<CharacteristicSet, Integer> subjectMap = new HashMap<CharacteristicSet, Integer>() ;
	
	public SQLTranslatorIS20() {
		
	}
	
	public SQLTranslatorIS20(Connection conn) {
		this.conn= conn;
	}
	
	public Map<CharacteristicSet, Integer> getSubjectMap() {
		return subjectMap;
	}

	public void setSubjectMap(Map<CharacteristicSet, Integer> subjectMap) {
		this.subjectMap = subjectMap;
	}

	Map<CharacteristicSet, List<Triple>> csVars = new HashMap<CharacteristicSet, List<Triple>>();
	
	Map<List<CharacteristicSet>, List<Triple>> csJoinProperties = new HashMap<List<CharacteristicSet>, List<Triple>>(); 
	


	public DirectedGraph<CharacteristicSet> getQueryGraph() {
		return queryGraph;
	}

	public void setQueryGraph(DirectedGraph<CharacteristicSet> queryGraph) {
		this.queryGraph = queryGraph;
	}

	public Map<CharacteristicSet, List<CharacteristicSet>> getCsJoinMap() {
		return csJoinMap;
	}

	public void setCsJoinMap(Map<CharacteristicSet, List<CharacteristicSet>> csJoinMap) {
		this.csJoinMap = csJoinMap;
	}

	public Set<CharacteristicSet> getCsSet() {
		return csSet;
	}

	public void setCsSet(Set<CharacteristicSet> csSet) {
		this.csSet = csSet;
	}

	public Map<CharacteristicSet, List<Triple>> getCsRestrictions() {
		return csRestrictions;
	}

	public void setCsRestrictions(Map<CharacteristicSet, List<Triple>> csRestrictions) {
		this.csRestrictions = csRestrictions;
	}

	public Map<String, Integer> getObjectMap() {
		return objectMap;
	}

	public void setObjectMap(Map<String, Integer> objectMap) {
		this.objectMap = objectMap;
	}

	public Map<CharacteristicSet, List<Triple>> getCsVars() {
		return csVars;
	}

	public void setCsVars(Map<CharacteristicSet, List<Triple>> csVars) {
		this.csVars = csVars;
	}

	public Map<List<CharacteristicSet>, List<Triple>> getCsJoinProperties() {
		return csJoinProperties;
	}

	public void setCsJoinProperties(Map<List<CharacteristicSet>, List<Triple>> csJoinProperties) {
		this.csJoinProperties = csJoinProperties;
	}

	Map<String, Integer> propertyMap ;

	public Map<String, Integer> getPropertyMap() {
		return propertyMap;
	}

	public void setPropertyMap(Map<String, Integer> propertyMap) {
		this.propertyMap = propertyMap;
	}

	public String getSparql() {
		return sparql;
	}

	public void setSparql(String sparql) {
		this.sparql = sparql;
	}
	
	public void parseSPARQL(){
		
		Query query = QueryFactory.create(sparql);		
		
		ElementGroup g = (ElementGroup) query.getQueryPattern() ;

		ElementPathBlock triplePathBlock = (ElementPathBlock) g.getElements().get(0);				
		
		Map<Node, List<Triple>> subjectTripleMap = new HashMap<Node, List<Triple>>();
		
		Set<Node> objects = new HashSet<Node>();
		
		List<Var> projectVars = query.getProjectVars() ;
		Set<String> varNames = new HashSet<String>();
		for(Var var : projectVars){
			varNames.add(var.getVarName());
		}
		//System.out.println("Vars: " + varNames.toString());
		
		for(TriplePath triplePath : triplePathBlock.getPattern().getList()){
			
			List<Triple> triplesOfSubject = subjectTripleMap.getOrDefault(triplePath.getSubject(), new ArrayList<Triple>());
			triplesOfSubject.add(triplePath.asTriple());
			subjectTripleMap.put(triplePath.getSubject(), triplesOfSubject);
			if(triplePath.getObject().isLiteral() || triplePath.getObject().isURI())
				objects.add(triplePath.getObject());			
			
		}
		
		Map<Node, List<Node>> varJoins = new HashMap<Node, List<Node>>();
		
		Map<Node, List<Triple>> subjectRestrictions = new HashMap<Node, List<Triple>>();
		
		Map<CharacteristicSet, List<CharacteristicSet>> csJoinMap = new HashMap<CharacteristicSet, List<CharacteristicSet>>();
		
		Map<Node, List<Integer>> subjectCSMap = new HashMap<Node, List<Integer>>();
		
		for(Node nextSubject : subjectTripleMap.keySet()){
						
			List<Integer> propertiesAsList = new ArrayList<Integer>();
			
			for(Triple nextTriple : subjectTripleMap.get(nextSubject)){
				
				Integer property = propertyMap.get("<" + nextTriple.getPredicate().toString()+">");
				
				propertiesAsList.add(property);
				
				//find joins											
				if(nextTriple.getObject().isVariable() && subjectTripleMap.containsKey(nextTriple.getObject())){
					
					List<Node> joins = varJoins.getOrDefault(nextSubject, new ArrayList<Node>());
					
					joins.add(nextTriple.getObject());
					
					varJoins.put(nextSubject, joins);
					
					
				}		
				else if(nextTriple.getObject().isURI() || nextTriple.getObject().isLiteral()){
					
					List<Triple> restrictions = subjectRestrictions.getOrDefault(nextSubject, new ArrayList<Triple>());
					
					restrictions.add(nextTriple);
					
					subjectRestrictions.put(nextSubject, restrictions);
					
				}
								
				
			}
			for(Triple nextTriple : subjectTripleMap.get(nextSubject)){
				if(nextTriple.getObject().isVariable() && !subjectTripleMap.containsKey(nextTriple.getObject())){
					
					
					if(varNames.contains(nextTriple.getObject().getName())){
						CharacteristicSet cs = new CharacteristicSet(propertiesAsList);
						//System.out.println("var is contained! " + nextTriple.getObject().getName());
						List<Triple> vars = csVars.getOrDefault(cs, new ArrayList<Triple>());
						
						vars.add(nextTriple);
						
						csVars.put(cs, vars);
					}
					
					
				}	
				
					
			}
			
			
			//System.out.println(csVars.toString());
			
			subjectCSMap.put(nextSubject, propertiesAsList);
			//System.out.println(subjectCSMap.toString()) ; 
			
			
			csSet.add(new CharacteristicSet(propertiesAsList));
						
			if(subjectRestrictions.containsKey(nextSubject))
				csRestrictions.put(new CharacteristicSet(propertiesAsList), subjectRestrictions.get(nextSubject));
			
			//System.out.println("Subject: " + nextSubject.toString());
			
			//System.out.println("\t"+subjectCSMap.get(nextSubject).toString());
									
		}
		
		for(Node nextSubject : subjectTripleMap.keySet()) {
			
			for(Triple nextTriple : subjectTripleMap.get(nextSubject)){				
				if(nextTriple.getObject().isVariable() && subjectTripleMap.containsKey(nextTriple.getObject())){
					
					List<CharacteristicSet> joinLists = new ArrayList<CharacteristicSet>();
					//System.out.println("left: " + propertiesAsList.toString());
					//System.out.println("right: " + subjectCSMap.get(nextTriple.getObject()).toString());
					joinLists.add(new CharacteristicSet(subjectCSMap.get(nextSubject)));
					joinLists.add(new CharacteristicSet(subjectCSMap.get(nextTriple.getObject())));
					List<Triple> joinTriples = csJoinProperties.getOrDefault(joinLists, new ArrayList<Triple>());
					
					joinTriples.add(nextTriple);
					
					csJoinProperties.put(joinLists, joinTriples);
					
				}
					
			}
			
		}
//		for(List<NewCS> nextJoin : csJoinProperties.keySet()){
//			System.out.println("next join: " + nextJoin);
//		}
//		System.out.println("csJoinProperties " + csJoinProperties.toString()) ;
		
		for(Node nextSubject : varJoins.keySet()){
						
			List<CharacteristicSet> joinedCS = new ArrayList<CharacteristicSet>();
				
			for(Node nextObject : varJoins.get(nextSubject)){
				joinedCS.add(new CharacteristicSet(subjectCSMap.get(nextObject)));
			}			
			
			csJoinMap.put(new CharacteristicSet(subjectCSMap.get(nextSubject)), joinedCS);
			
		}
		
		for(Node nextSubject : subjectCSMap.keySet()){
			
			if(csJoinMap.containsKey(new CharacteristicSet(subjectCSMap.get(nextSubject))))
				continue;
			csJoinMap.put(new CharacteristicSet(subjectCSMap.get(nextSubject)), null);
			
		}
		DirectedGraph<CharacteristicSet> queryGraph = new DirectedGraph<CharacteristicSet>();
		
		for(CharacteristicSet nextSubjectCS : csJoinMap.keySet()){
		
			queryGraph.addNode(nextSubjectCS);
			
			if(csJoinMap.get(nextSubjectCS) != null)
			for(CharacteristicSet nextJoin : csJoinMap.get(nextSubjectCS)){
				
				queryGraph.addNode(nextJoin);
				
				queryGraph.addEdge(nextSubjectCS, nextJoin);
				
			}
//			System.out.println(nextSubjectList);
//			if(csJoinMap.get(nextSubjectList) != null)
//				System.out.println("\t"+csJoinMap.get(nextSubjectList).toString());
//			
			
			
		}
		
		//System.out.println("Root nodes: " + queryGraph.findRoots().toString());
		
		this.queryGraph = queryGraph;
		
		this.csJoinMap = csJoinMap ;
		
		this.csSet = csSet ;
		
		objectMap = new HashMap<String, Integer>();
		
		for(Node nextObject : objects) {
			
			try{
				Statement st = conn.createStatement();
				
				String value = nextObject.toString();
				if(!nextObject.isLiteral())									
					value = "<" + value + ">";
				
				//System.out.println(value);
				
				//hash
//				String objectSetQuery = " SELECT id, label FROM dictionary WHERE label = " + value.hashCode() ;
				//strings
				String objectSetQuery = " SELECT id, label FROM dictionary WHERE label ='" + value.toString() +"'";//.hashCode() ;
				//System.out.println(objectSetQuery);
				ResultSet rsProps = st.executeQuery(objectSetQuery);
				
				while(rsProps.next()){
					objectMap.put(value, rsProps.getInt(1));
				}
				rsProps.close();
				st.close();
			}catch ( Exception e){
				e.printStackTrace();
			}
			
			//System.out.println(objectMap.toString()); 
			
		}
		//System.out.println("subject CS Map: " + subjectCSMap.toString()) ;
		for(Node nextSubject : subjectCSMap.keySet()) {
			
			try{
				Statement st = conn.createStatement();
				
				String value = nextSubject.toString();
				if(!nextSubject.isURI())									
					continue;
				value = "<" + value + ">";
				//hash
//				String objectSetQuery = " SELECT id, label FROM dictionary WHERE label = " + value.hashCode() ;
				//strings
				String objectSetQuery = " SELECT id, label FROM dictionary WHERE label ='" + value.toString() +"'";//.hashCode() ;
				
				//System.out.println(objectSetQuery);
				ResultSet rsProps = st.executeQuery(objectSetQuery);
				
				while(rsProps.next()){
					
					subjectMap.put(new CharacteristicSet(subjectCSMap.get(nextSubject)), rsProps.getInt(1));
				}
				rsProps.close();
				st.close();
			}catch ( Exception e){
				e.printStackTrace();
			}
			
			//System.out.println(objectMap.toString()); 
			
		}
		
	}
			
}
