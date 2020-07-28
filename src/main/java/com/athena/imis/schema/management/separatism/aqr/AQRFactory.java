package com.athena.imis.schema.management.separatism.aqr;

public class AQRFactory {

	public static IAQRManager createAQRManager(String projectFamily) {
		IAQRManager aqrManager = null;
		switch(projectFamily) {
			case "LUBM": 	aqrManager = new AQRManagerLubm();
							break;
							
			default:  	aqrManager = new AQRManagerLubm();
		}
		return aqrManager;
	}
}
