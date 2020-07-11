package com.athena.imis.schema.managment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DensityFactorOptimizerClientIS20 {
	private static final Logger LOG = LogManager.getLogger(DensityFactorOptimizerClientIS20.class);
	
	public static void main(String[] args) {
		

		int initialDensityFactor = 0;
		DensityFactorOptimizerIS20 schemaDecisionEngine = new DensityFactorOptimizerIS20(initialDensityFactor, args);
		int result = schemaDecisionEngine.optimizeDensityFactor();

		System.out.println("Density Factor returned " + result);
	}
	
}