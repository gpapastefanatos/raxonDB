package com.athena.imis.schema.managment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.athena.imis.models.Database;


public class DensityFactorOptimizerClientIS20 {
	private static final Logger LOG = LogManager.getLogger(DensityFactorOptimizerClientIS20.class);
	
	public static void main(String[] args) {
		
		String inFile = args[1];
		Database database = new Database(args[0], args[2], args[4], args[5], Integer.parseInt(args[3]));
		
		DensityFactorOptimizerIS20 schemaDecisionEngine = new DensityFactorOptimizerIS20(database, inFile);
		int result = schemaDecisionEngine.optimizeDensityFactor();

		System.out.println("Density Factor returned " + result);
	}
	
}