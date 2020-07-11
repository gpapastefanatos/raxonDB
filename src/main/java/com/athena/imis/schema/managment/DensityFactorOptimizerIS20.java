package com.athena.imis.schema.managment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.athena.imis.models.Database;
import com.athena.imis.querying.IRelationalQueryArray;
import com.athena.imis.querying.QueriesIS20;
import com.athena.imis.querying.RelationalQueryArrayIS20;
import com.athena.imis.querying.QueriesIS20.Dataset;
import com.esotericsoftware.minlog.Log;

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
	private int densityFactor ; 
	private int currentNullRecords = 0;
	private int currentNoOfTables = 0;;
	
	private String[] args ;
	private Database database;
	Connection conn ;
	
	private static final Logger LOG = LogManager.getLogger(DensityFactorOptimizerIS20.class);

	
	public DensityFactorOptimizerIS20(int densityFactor, String[] args) {
		this.densityFactor = densityFactor;
		this.args = args;
		this.database = new Database(args[0], args[2], args[4], args[5]);
		 
	}

	public CostBasedSchemaManagementDOLAP_Analyze getSchemaBuilder() {
		return schemaBuilder;
	}


	public IRelationalQueryArray getQueryBuilder() {
		return queryBuilder;
	}


	public int getDensityFactor() {
		return densityFactor;
	}


	public void setDensityFactor(int densityFactor) {
		this.densityFactor = densityFactor;
	}

	
	public int optimizeDensityFactor() {
		int optimaldensityFactor = this.densityFactor;
		int maxDensityNullCost = Integer.MAX_VALUE; 
		
		String report="DensityFactor\tNoOfTables\tNoOfNulls\tQueryCost(ms)\tDensityCost\t";
//		LOG.info(report);
		Map<String, Integer> tableExtents =this.initialize();
		LOG.debug("Densities Initialization Complete");			
		
		for (int densFactor = 0; densFactor >=0; densFactor+=10) {
			//DIAGNOSTICS		
			LOG.debug("-----------START OF ITERATION FOR DENSITY FACTOR = " + densFactor + " --------------");

			args[6] = new Integer(densFactor).toString();	
			
					
			schemaBuilder = new CostBasedSchemaManagementDOLAP_Analyze(args, this.database);
			schemaBuilder.decideSchemaAndPopulate();
			
			
			//calculate last schema's aggregate no of null columns
			 currentNullRecords = 0;
			 currentNoOfTables = 0;
			this.calculateSchemaCosts();
			//nullcost is set to the cost of the best performing schema
			LOG.debug("--DENSITY FACTOR = " + densFactor + ": Best NullCost=" + maxDensityNullCost + " and Current CS Cost =" + currentNullRecords);
			if(currentNullRecords<=maxDensityNullCost){
				maxDensityNullCost=currentNullRecords;
				optimaldensityFactor = densFactor;
			} 

			LOG.info("--DENSITY FACTOR = " + densFactor);
			float costOfWorkload = this.getQueryCost(new QueriesIS20(), Dataset.LUBM1);
			float densityCost =  (float)currentNullRecords /costOfWorkload;
			report= densFactor+"\t"+currentNoOfTables+"\t"+currentNullRecords+"\t"+costOfWorkload+"\t"+densityCost;
//			LOG.info(report);

			/*TODO
			 * Here we will perfrom a simulation annealing function for finding the optimized m
			 */
			//LOG.info("-----------END OF ITERATION FOR DENSITY FACTOR = " + densFactor + " --------------");	

		}
		
				
		return optimaldensityFactor;
		
	}
	
	
	/***
	 *  It initializes db with initial density factor equals to 0, ie., each CS forms a separate table. 
	 * @return a map CS table name to their extent 
	 * 
	 */
	private Map<String, Integer> initialize() {
		Map<String, Integer> tableExtent = new HashMap<String, Integer>();
		
		//set the density factor to 0 such that schema is created with no merges 
		int defaultDensityFactor = 0;
		args[6] = new Integer(defaultDensityFactor).toString();	
		
						
		schemaBuilder = new CostBasedSchemaManagementDOLAP_Analyze(args, this.database);
		schemaBuilder.decideSchemaAndPopulate();
		
		//get the size of each table in the db
		try {
			conn = database.getConnection(conn);
			Statement st = conn.createStatement();
			// get the schema from the CS dictionary table
			String CSschema = " SELECT id FROM cs_schema ORDER BY id ASC;";
			ResultSet rsCs = st.executeQuery(CSschema);

			while(rsCs.next()){
				//build a query for each of the CS table and count the null cells
				String csTableName = "cs_"+rsCs.getInt(1);
				Statement st2 = conn.createStatement();
				String sql = "SELECT count(*) FROM " + csTableName;
				ResultSet rsTable = st2.executeQuery(sql);
				while(rsTable.next()){
					tableExtent.put(csTableName, rsTable.getInt(1));
				}
				rsTable.close();
				st2.close();
			}
			rsCs.close();
			st.close();
		}
		catch(SQLException e){
			e.printStackTrace();
		}
			
		return tableExtent;
	}
	
	/***
	 * Method for calculating the number of columns with nulls in all tables in the current schema 
	 * 
	 ***/
	private void calculateSchemaCosts() {
		  
		
		try {
			conn = database.getConnection(conn);
			Statement st = conn.createStatement();
			// get the schema from the CS dictionary table
			String CSschema = " SELECT id, properties FROM cs_schema ORDER BY id ASC;";
			ResultSet rsCs = st.executeQuery(CSschema);

			while(rsCs.next()){
				//build a query for each of the CS table and count the null cells
				String csTableName = "cs_"+rsCs.getInt(1);
				currentNoOfTables++;
				List<String> csColumns  = new ArrayList<String>(Arrays.asList(rsCs.getString(2).replace("{","").replace("}", "").split(",")));
				
				for (String column : csColumns) {
					Statement st2 = conn.createStatement();
					String sql = "SELECT count(*) FROM " + csTableName + " WHERE p_" + column + " IS NULL";
					ResultSet rsNULLs = st2.executeQuery(sql);
					while(rsNULLs.next()){
						currentNullRecords +=rsNULLs.getInt(1);
					}
					rsNULLs.close();
					st2.close();
				}
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
	private float getQueryCost(QueriesIS20 queries, Dataset dataset) {
		
		float cost = (float) 0.0;
		//calculate last schema's aggregate WORKLOAD cost
		
		//define a query Builder 
		String[] queryArgs = {args[0],args[2],args[4],args[5]};
		
		
		
		this.queryBuilder = new RelationalQueryArrayIS20(this.database);
		int i = 1;
		for(String sparql : queries.getQueries(Dataset.LUBM1)){
			//run only the i-th query in the Queries.getquery list
			
			LOG.debug("Syntax of SPARQL:\t" + sparql);
			String sql = queryBuilder.generateSQLQuery(sparql);
			LOG.debug("Syntax of SQL:\t" +sql);
			
			LOG.info("Syntax of SQL:\t" +sql);
			
			float execTime = 0;
			float planTime = 0;
//			try {
//				conn = database.getConnection(conn);
//				Statement st2= conn.createStatement();
//				String explain = "EXPLAIN ANALYZE " +sql ;
//				ResultSet rs2 = st2.executeQuery(explain); //
//				LOG.debug("Start Q" + i + " execution plan");
//				
//				while (rs2.next())
//				{	
//					//prints the planner's tree
//					LOG.debug(rs2.getString(1));
//					if(rs2.getString(1).contains("Execution Time: ")){
//						String exec = rs2.getString(1).replaceAll("Execution Time: ", "").replaceAll("ms", "").trim();
//						execTime += Float.parseFloat(exec);
//
//					}
//					else if(rs2.getString(1).contains("Planning Time: ")){
//						String plan = rs2.getString(1).replaceAll("Planning Time: ", "").replaceAll("ms", "").trim();
//						planTime += Float.parseFloat(plan);
//					}					   
//				}
//				float q_totalTime= planTime+execTime;
//				rs2.close();
//				st2.close();
//				LOG.debug("Q" + i + ":\tPlanTime\t" + planTime + "ms\tExecTime\t" + execTime + "ms\tTotalTime\t" + q_totalTime + "ms");
//				i++;
//				
//				cost+= q_totalTime;
// 
//				//test the first i-th queries
////				if ((i>8)) { 
////					break;}
//			
//
//
//			} catch ( Exception e ) {
//				Log.error(e.toString());
//				//System.err.println( e.getClass().getName()+": "+ e.getMessage() );
//				//System.exit(0);
//			}
		}			
		return cost;

		
	}


}
