package com.athena.imis.querying.extras;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.athena.imis.models.Database;
import com.athena.imis.querying.common.IRelationalQueryArray;
import com.athena.imis.querying.density.RelationalQueryArrayIS20;
import com.athena.imis.querying.extras.QueriesIS20.Dataset;
import com.esotericsoftware.minlog.Log;

public class SimpleClientQueryIS {

	private static final Logger LOG = LogManager.getLogger(SimpleClientQueryIS.class);
	/**
	 * @param args A String array with the following format, describe via an example: 
	 *	e.g. 195.251.63.129 or localhost  -- where the pg server runs 
	 *	e.g. lubm  						  -- the name of the db
	 *	e.g., postgres 					  -- db username 
	 *	e.g., mypassword				  -- db password 
	 */	
	public static void main(String[] args) {
		
		Connection conn;
		
		//define a query Builder 
		Database d =  new Database(args[0], args[1], args[2], args[3]);

		IRelationalQueryArray queryBuilder = new RelationalQueryArrayIS20(d);
		int i = 1;
		QueriesIS20 queries  = new QueriesIS20();
		for(String sparql : queries.getQueries(Dataset.LUBM1)){
			//run only the i-th query in the Queries.getquery list
			
			LOG.info("Syntax of SPARQL:\t" + sparql);
			String sql = queryBuilder.generateSQLQuery(sparql);
			LOG.info("Syntax of SQL:\t" +sql);
			
			float execTime = 0;
			float planTime = 0;
			Statement st2;
			try {
				Class.forName("org.postgresql.Driver");
				conn = DriverManager
						.getConnection("jdbc:postgresql://"+args[0]+":5432/" + args[1].toLowerCase(), args[2], args[3]);
				st2 = conn.createStatement();

				String explain = "EXPLAIN ANALYZE " +sql ;

				ResultSet rs2 = st2.executeQuery(explain); //
				LOG.info("Start Q" + i + " execution plan");
				
				while (rs2.next())
				{	
					//prints the planner's tree
					LOG.info(rs2.getString(1));
					if(rs2.getString(1).contains("Execution Time: ")){
						String exec = rs2.getString(1).replaceAll("Execution Time: ", "").replaceAll("ms", "").trim();
						execTime += Float.parseFloat(exec);

					}
					else if(rs2.getString(1).contains("Planning Time: ")){
						String plan = rs2.getString(1).replaceAll("Planning Time: ", "").replaceAll("ms", "").trim();
						planTime += Float.parseFloat(plan);
					}					   
				}
				float totalTime= planTime+execTime;
				rs2.close();
				LOG.info("Q" + i + ":\tPlanTime\t" + planTime + "ms\tExecTime\t" + execTime + "ms\tTotalTime\t" + totalTime + "ms");
				conn.close();
				i++;
				
				
				//test the first i-th queries
//				if ((i>8)) { 
//					break;}
			


			} catch ( Exception e ) {
				Log.error(e.toString());
				//System.err.println( e.getClass().getName()+": "+ e.getMessage() );
				//System.exit(0);
			}
		}
	}
}

		