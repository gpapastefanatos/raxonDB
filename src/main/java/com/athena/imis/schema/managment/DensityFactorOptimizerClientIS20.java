package com.athena.imis.schema.managment;


public class DensityFactorOptimizerClientIS20 {

	public static void main(String[] args) {
		int initialDensityFactor = 0;
		DensityFactorOptimizerIS20 schemaDecisionEngine = new DensityFactorOptimizerIS20(initialDensityFactor, args);
		int result = schemaDecisionEngine.optimizeDensityFactor();

		System.out.println("Density Factor returned " + result);
	}
	
}