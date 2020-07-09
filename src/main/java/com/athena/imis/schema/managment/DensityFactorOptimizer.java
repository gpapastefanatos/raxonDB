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

import com.athena.imis.models.CharacteristicSet;
import com.athena.imis.querying.RelationalQueryArray;

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
 * The specific optimization problem finds a Pareto equilibrium between conciseness and performance of a relational schema (few tables) 
 * and internal density (with respect to the empty space they contain).  
 * 
 *  The cost(S) for m=0 (all CSs are stored in their own table) is 0 (no null values exist). The extent of the different
 * 	tables is used as the candidate space of values for m. So if we have 5 CSs with 5 different number of records, t   
 * 		
 * 
 * @author Gpapas
 *
 */
public class DensityFactorOptimizer {

	private CostBasedSchemaManagementDOLAP20 schemaBuilder; 
	private RelationalQueryArray queryBuilder; 
	private int densityFactor ; 
	private String[] args ;
	private static final Logger LOG = LogManager.getLogger(DensityFactorOptimizer.class);

	
	public DensityFactorOptimizer(int densityFactor, String[] args) {
		
		this.queryBuilder = new RelationalQueryArray();
		this.densityFactor = densityFactor;
		this.args = args;
		
		
		

	}


	public CostBasedSchemaManagementDOLAP20 getSchemaBuilder() {
		return schemaBuilder;
	}


	public RelationalQueryArray getQueryBuilder() {
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
		int nullCost = Integer.MAX_VALUE; 
		
		Map<String, Integer> tableExtents =this.initialize();
		
		
		
		
		for (int densFactor = 0; densFactor <=100; densFactor+=25) {
			//DIAGNOSTICS		
      		//LOG.info("-----------START OF ITERATION FOR DENSITY FACTOR = " + densFactor + " --------------");

			args[6] = new Integer(densFactor).toString();	
			
			schemaBuilder = new CostBasedSchemaManagementDOLAP20(args);
			schemaBuilder.decideSchemaAndPopulate();
		
			//calculate last schema's aggregate no of null columns
			int noOfNulls= countNullColumns();
			//nullcost is set to the cost of the best performing schema
			LOG.info("--DENSITY FACTOR = " + densFactor + ": Best NullCost=" + nullCost + " and Current CS Cost =" + noOfNulls);
			if(noOfNulls<=nullCost){
				nullCost=noOfNulls;
				optimaldensityFactor = densFactor;
			} 

			
			//calculate last schema's aggregate WORKLOAD cost
			queryBuilder.main(args); 
			
			int costOfWorkload = (int) (Math.random()*100);
			
			float costOfDensity = costOfWorkload / nullCost;
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
		
		//create the initial schema
		schemaBuilder = new CostBasedSchemaManagementDOLAP20(args);
		schemaBuilder.decideSchemaAndPopulate();
		
		//get the size of each table in the db
		
		try {
			Connection conn = this.getConnection();
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
			conn.close();
		}catch(SQLException e){
			e.printStackTrace();
		}
			
		return tableExtent;
	}
	
	/***
	 * Methods for calculating the number of columns with nulls in all tables in the current schema 
	 * 
	 ***/
	private int countNullColumns() {
		  
		int nullRecords = 0;
		
		try {
			Connection conn = this.getConnection();
			Statement st = conn.createStatement();
			// get the schema from the CS dictionary table
			String CSschema = " SELECT id, properties FROM cs_schema ORDER BY id ASC;";
			ResultSet rsCs = st.executeQuery(CSschema);

			while(rsCs.next()){
				//build a query for each of the CS table and count the null cells
				String csTableName = "cs_"+rsCs.getInt(1);
				List<String> csColumns  = new ArrayList<String>(Arrays.asList(rsCs.getString(2).replace("{","").replace("}", "").split(",")));
				
				for (String column : csColumns) {
					Statement st2 = conn.createStatement();
					String sql = "SELECT count(*) FROM " + csTableName + " WHERE p_" + column + " IS NULL";
					ResultSet rsNULLs = st2.executeQuery(sql);
					while(rsNULLs.next()){
						nullRecords +=rsNULLs.getInt(1);
					}
					rsNULLs.close();
					st2.close();
				}
			}
			rsCs.close();
			st.close();
			conn.close();
		}catch(SQLException e){
			e.printStackTrace();
		}
			
		return nullRecords;
		
	}
	
	
	private Connection getConnection() throws SQLException {

		Connection conn = null;
		// calculate null columns in each table in the db
		try{

			Class.forName("org.postgresql.Driver");
			conn = DriverManager
					.getConnection("jdbc:postgresql://"+args[0]+":5432/" + args[2].toLowerCase(), args[4], args[5]);
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			System.exit(0);
		}	

		return conn;
	}
}
