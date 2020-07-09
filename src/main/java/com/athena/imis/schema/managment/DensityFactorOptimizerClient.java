package com.athena.imis.schema.managment;



public class DensityFactorOptimizerClient {

	public static void main(String[] args) {
		int initialDensityFactor = 0;
		DensityFactorOptimizer schemaDecisionEngine = new DensityFactorOptimizer(initialDensityFactor, args);
		int result = schemaDecisionEngine.optimizeDensityFactor();

		System.out.println("Density Factor returned " + result);
	}
}