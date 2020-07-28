package com.athena.imis.schema.management.density;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.athena.imis.experiments.common.QueriesIS20ForLubm;
import com.athena.imis.models.CharacteristicSet;
import com.athena.imis.models.Database;
import com.athena.imis.querying.common.IRelationalQueryArray;
import com.athena.imis.querying.density.RelationalQueryArrayIS20;

import org.apache.commons.lang3.StringUtils;

/***
 * Optimizes the Density Factor m , i.e. the threshold for dense nodes in the schema creation (CostBasedSchemaManagementDOLAP20.class)  
 * such that the cost incurred by each schema produced is minimized.
 * 
 * Let each m produces a single schema S , then the cost is defined in terms of two factors 
 * 			a) how many columns with null values are produced due to merging of CSs in S : Null Cost = Σ (columns_null). 
 * 			b) estimation of the cost of the workload applied on S, The cost of a query Q over a relational DB is estimated
 * 				by the no of records read (I\O) by each hash join (3M+N) contained in the query. QueryCost = Σ (Join Cost)
 *
 * The Cost of m is a function Cost(S, Q, D) = NullCost/QueryCost with m = argmin Cost(S, Q, D) i.e., it captures the notion that 
 * for a given workload Q over a dataset D,  a schema S resulting from the density factor m is optimal, 
 * if the ratio of query performance over the schema density is maximized. I.e., we achieve the best execution performance of Q 
 * over a most dense schema over D. 
 * 
* 
 *  The cost(S) for m=0 (all CSs are stored in their own table) is 0 (no null values exist). The extent of the different
 * 	tables is used as the candidate space of values for m. So if we have 5 CSs with different number of records, then m takes one of the 5 values   
 * 		
 * 
 * @author Gpapas
 *
 */
public class DensityFactorOptimizerIS20 {

	private CostBasedSchemaManagementDOLAP_Analyze schemaBuilder; 
	private IRelationalQueryArray queryBuilder; 
	private int optimalDensityFactor = 0; ; 
	
	private Map<String, List<Integer>> tableStats = new HashMap<String, List<Integer>>();
	private int schemaNullColumns = 0;
	private int currentNoOfTables = 0;;
	
	private Database database;
	private String inFileName = "";
	private QueriesIS20ForLubm queries ; 
	Connection conn ;
	private float queryCost[];
	private Map<String, Map<String,Integer>> queryStats = new HashMap<String, Map<String,Integer>>(); 
	private int queryStat[][] ;
	
	private List<String> sparqlQueries;
	private String report ;
	private static final Logger LOG = LogManager.getLogger(DensityFactorOptimizerIS20.class);
	
	
	public DensityFactorOptimizerIS20(Database database, String inputFilename) {
		this.inFileName = inputFilename;
		this.database = database;
		this.queries = new QueriesIS20ForLubm();
		this.sparqlQueries = queries.getQueries(com.athena.imis.experiments.common.QueriesIS20ForLubm.Dataset.LUBM1);
		this.report = "";
		 
	}

 
	public CostBasedSchemaManagementDOLAP_Analyze getSchemaBuilder() {
		return schemaBuilder;
	}


	public IRelationalQueryArray getQueryBuilder() {
		return queryBuilder;
	}


	public int getOptimalDensityFactor() {
		return optimalDensityFactor;
	}


	public void setOptimalDensityFactor(int densityFactor) {
		this.optimalDensityFactor = densityFactor;
	}

	
	public int optimizeDensityFactor() {
		
		int maxDensityNullCost = Integer.MAX_VALUE; 
		
//		report="DensityFactor\tNoOfTables\tNoOfNulls\tq1\tq2\tq4_ext\tq7\tq9\tq10\tQueryCost(ms)\tDensityCost";
//		LOG.info(report);
		
		Map<CharacteristicSet, Integer> tableExtents =this.initialize();
		LOG.debug("Densities Initialization Complete");			
		
//		find maxTablesize from tableExtens and initialize the search space of density
		int maxTableSize = 0;
		for (CharacteristicSet table : tableExtents.keySet()) {
			maxTableSize = Math.max(maxTableSize, tableExtents.get(table));
		}

		Set<Integer> densFactors = new TreeSet<Integer>();
		for (CharacteristicSet table : tableExtents.keySet()) {
			densFactors.add((int) Math.round((double)tableExtents.get(table)*100/(double)maxTableSize));
		}
		
//		densFactors.add(0);
//		densFactors.add(1);
//		densFactors.add(3);
//		densFactors.add(6);
//		densFactors.add(7);
//		densFactors.add(19);
//		densFactors.add(24);
//		densFactors.add(25);
//		densFactors.add(78);
		///densFactors.add(100);
		
		
//		densFactors.add(1);
//		densFactors.add(2);
//		densFactors.add(3);
//		densFactors.add(9);
//		densFactors.add(10);
//		densFactors.add(13);
//		densFactors.add(14);
//		densFactors.add(29);
//		densFactors.add(37);
//		densFactors.add(61);
		
//		densFactors.add(100);

		
		//loop through densities
		for (int densFactor:densFactors) {
			//DIAGNOSTICS		
			LOG.debug("-----------START OF ITERATION FOR DENSITY FACTOR = " + densFactor + " --------------");
			//change default density factor for db
//			//change db name to create _suffixed with density factor
//			this.database.setDbName(args[2] + "_" + densFactor);
//			this.conn = null;			
					
			schemaBuilder = new CostBasedSchemaManagementDOLAP_Analyze(this.database, this.inFileName, densFactor);
			schemaBuilder.decideSchemaAndPopulate();
			
			
			//calculate last schema's aggregate no of null columns
			 schemaNullColumns = 0;
			 currentNoOfTables = 0;
			 this.calculateSchemaCosts();
			
			//report tables stats for this schema
			LOG.info("DenFactor:" + densFactor);
			for (String table: tableStats.keySet()) {
				report = table+":\t"+tableStats.get(table).get(0)+"\t"+tableStats.get(table).get(1);
				LOG.info(report);
			}
			
			//nullcost is set to the cost of the best performing schema
			LOG.debug("--DENSITY FACTOR = " + densFactor + ": Best NullCost=" + maxDensityNullCost + " and Current CS Cost =" + schemaNullColumns);
			if(schemaNullColumns<=maxDensityNullCost){
				maxDensityNullCost=schemaNullColumns;
				optimalDensityFactor = densFactor;
			} 

			LOG.debug("--DENSITY FACTOR = " + densFactor);
			
					
			float costOfWorkload = this.calculateQueryCosts();
			float densityCost =  (float)schemaNullColumns /costOfWorkload;
		
			//report query stats for this schema
			report ="";
			for (int index =0 ; index< queryCost.length; index++) {
				report += "Q" +index + "\t";
			}
			LOG.info(report);
			report ="";
			for (float cost : queryCost) {
				report += cost +"\t";
			}
			LOG.info(report);
			report ="";
			for (int index =0 ; index< queryStat[0].length; index++) { 
					report += "C" + index + "\t";
			}
			LOG.info(report);
			report ="";
			for (int[] tables : queryStat) {
				for (int j : tables) { 
					report +=  + j + "\t";
				}
				LOG.info(report);
				report ="";
			}
			

			
			/*TODO
			 * Here we will perfrom a simulation annealing function for finding the optimized m
			 */
			
			
			
			LOG.debug("-----------END OF ITERATION FOR DENSITY FACTOR = " + densFactor + " --------------");	

		}
		
				
		return optimalDensityFactor;
		
	}
	
	
	private double getDensityCost(double densityFactor) {
		double cost = 0;
//		if densityFactor<0.1
		
		return cost;
		
		
	}
	
	private double optimizeSA (double[] densityStates) {
		
		 // Set initial temp
        double temp = 100;

        // Cooling rate
        double coolingRate = 5;

       //set initial densityFactor
        double bestDensity = 0;
        double bestCost = getDensityCost(bestDensity);
     
        double nextDensity  ;
        double nextCost;         
        // Loop until system has cooled
        while (temp > 1) {
        	
        	nextDensity = bestDensity+0.05;
            nextCost = getDensityCost(bestDensity);
             

            // Decide if we should accept the neighbour
            if (acceptanceProbability(bestCost, nextCost, temp) > Math.random()) {
            	bestDensity = nextDensity;
            }

                    
            // Cool system
            temp *= 1-coolingRate;
        }
        return bestDensity;
		
	}
	
	// Calculate the acceptance probability
    public static double acceptanceProbability(double bestCost, double nextCost, double temperature) {
        // If the new solution is better, accept it
        if (nextCost < bestCost) {
            return 1.0;
        }
        // If the new solution is worse, calculate an acceptance probability
        return Math.exp((bestCost - nextCost) / temperature);
    }
    
	
	/***
	 *  It initializes db with initial density factor equals to 0, ie., each CS forms a separate table. 
	 * @return a map CS table name to their extent 
	 * 
	 */
	private Map<CharacteristicSet, Integer> initialize() {
		
		//set the density factor to 0 such that schema is created with no merges 
		int defaultDensityFactor = 0;
						
		schemaBuilder = new CostBasedSchemaManagementDOLAP_Analyze(this.database, this.inFileName, defaultDensityFactor);
		schemaBuilder.decideSchemaAndPopulate();
		return 		schemaBuilder.csExtentSizes;

		
//		//get the size of each table in the db
//		try {
//			conn = database.getConnection(conn);
//			Statement st = conn.createStatement();
//			// get the schema from the CS dictionary table
//			String CSschema = " SELECT id FROM cs_schema ORDER BY id ASC;";
//			ResultSet rsCs = st.executeQuery(CSschema);
//
//			while(rsCs.next()){
//				//build a query for each of the CS table and count the null cells
//				String csTableName = "cs_"+rsCs.getInt(1);
//				Statement st2 = conn.createStatement();
//				String sql = "SELECT count(*) FROM " + csTableName;
//				ResultSet rsTable = st2.executeQuery(sql);
//				while(rsTable.next()){
//					tableExtent.put(csTableName, rsTable.getInt(1));
//				}
//				rsTable.close();
//				st2.close();
//			}
//			rsCs.close();
//			st.close();
//		}
//		catch(SQLException e){
//			e.printStackTrace();
//		}
//			
//		return tableExtent;
	}
	
	/***
	 * Method for calculating the number of columns with nulls in all tables in the current schema 
	 * 
	 ***/
	private void calculateSchemaCosts() {
		
		tableStats.clear();
		
		try {
			conn = database.getConnection(conn);
			Statement st = conn.createStatement();
			// get the schema from the CS dictionary table
			String CSschema = " SELECT id, properties FROM cs_schema ORDER BY id ASC;";
			ResultSet rsCs = st.executeQuery(CSschema);

			while(rsCs.next()){
				//build a query for each of the CS table and count the null cells
				String csTableName = "cs_"+rsCs.getInt(1);
				List<Integer> tableCounts = new ArrayList<Integer>();

				Statement st2 = conn.createStatement();
				String sql = "SELECT count(*) FROM " + csTableName;
				ResultSet rsCount = st2.executeQuery(sql);
				while(rsCount.next()){
					tableCounts.add(rsCount.getInt(1));
				}
				rsCount.close();
				st2.close();
				
				int tableNullCells = 0;
				List<String> csColumns  = new ArrayList<String>(Arrays.asList(rsCs.getString(2).replace("{","").replace("}", "").split(",")));
				for (String column : csColumns) {
					Statement st3 = conn.createStatement();
					String sql2 = "SELECT count(*) FROM " + csTableName + " WHERE p_" + column + " IS NULL";
					ResultSet rsNULLs = st3.executeQuery(sql2);
					while(rsNULLs.next()){
						tableNullCells +=rsNULLs.getInt(1);
					}
					rsNULLs.close();
					st3.close();
				}				
				tableCounts.add(tableNullCells);
				tableStats.put(csTableName, tableCounts);
				currentNoOfTables++;
				schemaNullColumns+=tableNullCells;
			}
			
			rsCs.close();
			st.close();
			
			
		}catch(SQLException e){
			e.printStackTrace();
		}
			
		
		
	}
	
	/***
	 * Method for calculating the cost of sparql queries in Queries static collection expressed over a dataset 
	 *   It transforms the sparql query to the sql expression and it executes it over the current underlying CS schema
	 * 
	 ***/
	private float calculateQueryCosts() {
		
		queryStats.clear(); 
		queryStat = new int[sparqlQueries.size()][currentNoOfTables] ;
		queryCost = new float[sparqlQueries.size()];
		
		float totalQueryCost = (float) 0.0;
		//calculate last schema's aggregate WORKLOAD cost
		
		
		this.queryBuilder = new RelationalQueryArrayIS20(this.database);
		int i = 0;
		for(String sparql : sparqlQueries){
			//run only the i-th query in the Queries.getquery list
//			if (i >1) break;
//			System.out.println("Processing:\t" +sparql);
			String sql = queryBuilder.generateSQLQuery(sparql);

			LOG.info("Q"+i +"\t" +sql);

			float execTime = 0;
			float planTime = 0;
			try {
				conn = database.getConnection(conn);
				Statement st2= conn.createStatement();
				String explain = "EXPLAIN ANALYZE " +sql ;
				ResultSet rs2 = st2.executeQuery(explain); //
				LOG.debug("Start Q" + i + " execution plan");

				while (rs2.next())
				{	
					//prints the planner's tree
					LOG.debug(rs2.getString(1));
					if(rs2.getString(1).contains("Execution Time: ")){
						String exec = rs2.getString(1).replaceAll("Execution Time: ", "").replaceAll("ms", "").trim();
						execTime += Float.parseFloat(exec);

					}
					else if(rs2.getString(1).contains("Planning Time: ")){
						String plan = rs2.getString(1).replaceAll("Planning Time: ", "").replaceAll("ms", "").trim();
						planTime += Float.parseFloat(plan);
					}					   
				}
				rs2.close();
				st2.close();
			} catch ( Exception e ) {
				LOG.error(e.toString());
				report += "\t";
			}
			queryCost[i]= planTime+execTime;
			LOG.debug("Q" + i + ":\tPlanTime\t" + planTime + "ms\tExecTime\t" + execTime + "ms\tTotalTime\t" + queryCost[i] + "ms");

			totalQueryCost+= queryCost[i];

			for (String tableName: tableStats.keySet()) {
				queryStat[i][new Integer(tableName.substring(tableName.length()-1))] = StringUtils.countMatches(sql, tableName);
			}
			i++;
		}		
		

		return totalQueryCost;

		
	}


}
