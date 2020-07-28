package com.athena.imis.experiments.density;

import com.athena.imis.schema.management.density.DensityFactorOptimizerIS20;

public class DensityFactorOptimizerClientIS20 {

	public static void main(String[] args) {
		int initialDensityFactor = 0;
		DensityFactorOptimizerIS20 schemaDecisionEngine = new DensityFactorOptimizerIS20(initialDensityFactor, args);
		int result = schemaDecisionEngine.optimizeDensityFactor();

		System.out.println("Density Factor returned " + result);
	}
	
}