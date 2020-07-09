package com.athena.imis.schema.managment.aqr;

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
