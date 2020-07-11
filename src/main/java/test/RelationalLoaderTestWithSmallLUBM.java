/**
 * 
 */
package test;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.athena.imis.schema.management.CostBasedSchemaManagementIS20;

/**
 * Just a very small data set lubm nt 2
 * 
 * @author pvassil
 *
 */
class RelationalLoaderTestWithSmallLUBM {

	public static CostBasedSchemaManagementIS20 schemaDecisionEngine;
	public static String[] args = {
			"localhost", 
			"resources/datasets/lubm/lubm2.nt", 
			"TestDBRaxonLubm2", 
			"100", 
			"postgres", 
			"postgres", 
			"2"		
	};
	public static Connection conn=null;
	public static int dbCreationResult = -99;
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		
		schemaDecisionEngine = new CostBasedSchemaManagementIS20(args);
		dbCreationResult = schemaDecisionEngine.decideSchemaAndPopulate();

		conn = null;
		try{

			Class.forName("org.postgresql.Driver");
			conn = DriverManager
					.getConnection("jdbc:postgresql://"+args[0]+":5432/" + args[2].toLowerCase(), args[4], args[5]);
			System.out.println("\n\n\nOpened CHECK database successfully");
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			System.exit(0);
		}	

	}



	/**
	 * Test method for {@link com.athena.imis.schema.management.CostBasedSchemaManagementIS20#decideSchemaAndPopulate()}.
	 */
	@Test
	void testDecideSchemaAndPopulate() {
		assertEquals(0, dbCreationResult);
	}
	
	@Test
	void testNumberOfEntriesTablePropertySet() {
		int rsSize =0;
		try{
			Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);  //to allow the last() call
			String propertiesSetQuery = " SELECT id, uri FROM propertiesset ;";
			ResultSet rsProps = st.executeQuery(propertiesSetQuery);

//			while(rsProps.next()){
//				System.out.println(rsProps.getInt(1) + "\t\t" + rsProps.getString(2));
//			}
			
			if (rsProps != null) 
			{
			  rsProps.last();    // moves cursor to the last row
			  rsSize = rsProps.getRow(); // get row id 
			}
			
			rsProps.close();
			st.close();
		}catch(SQLException e){
			e.printStackTrace();
		}
		assertEquals(2, rsSize);
	}//end method

	@Test
	void testNumberOfEntriesTableCS0() {
		int rsSize =0;
		try{
			Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);  //to allow the last() call
			String propertiesSetQuery = " SELECT s, p_0, p_1 FROM cs_0 ORDER BY s ASC;";
			ResultSet rsProps = st.executeQuery(propertiesSetQuery);

//			while(rsProps.next()){
//				System.out.println(rsProps.getInt(1) + "\t\t" + rsProps.getString(2));
//			}
			
			if (rsProps != null) 
			{
			  rsProps.last();    // moves cursor to the last row
			  rsSize = rsProps.getRow(); // get row id 
			}
			
			rsProps.close();
			st.close();
		}catch(SQLException e){
			e.printStackTrace();
		}
		assertEquals(2, rsSize);
	}//end method
	
	@Test
	void testNumberOfEntriesTableCS1() {
		int rsSize =0;
		try{
			Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);  //to allow the last() call
			String propertiesSetQuery = " SELECT s, p_0 FROM cs_1 ORDER BY s ASC;";
			ResultSet rsProps = st.executeQuery(propertiesSetQuery);

//			while(rsProps.next()){
//				System.out.println(rsProps.getInt(1) + "\t\t" + rsProps.getString(2));
//			}
			
			if (rsProps != null) 
			{
			  rsProps.last();    // moves cursor to the last row
			  rsSize = rsProps.getRow(); // get row id 
			}
			
			rsProps.close();
			st.close();
		}catch(SQLException e){
			e.printStackTrace();
		}
		assertEquals(1, rsSize);
	}//end method

	@Test
	void testNumberOfEntriesTableCSSCHEMA() {
		int rsSize =0;
		try{
			Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);  //to allow the last() call
			String propertiesSetQuery = " SELECT id, properties FROM cs_schema ORDER BY id ASC;";
			ResultSet rsProps = st.executeQuery(propertiesSetQuery);

//			while(rsProps.next()){
//				System.out.println(rsProps.getInt(1) + "\t\t" + rsProps.getString(2));
//			}
			
			if (rsProps != null) 
			{
			  rsProps.last();    // moves cursor to the last row
			  rsSize = rsProps.getRow(); // get row id 
			}
			
			rsProps.close();
			st.close();
		}catch(SQLException e){
			e.printStackTrace();
		}
		assertEquals(2, rsSize);
	}//end method	

	@Test
	void testNumberOfEntriesTableDICTIONARY() {
		int rsSize =0;
		try{
			Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);  //to allow the last() call
			String propertiesSetQuery = " SELECT id, label FROM dictionary ORDER BY id ASC;";
			ResultSet rsProps = st.executeQuery(propertiesSetQuery);

//			while(rsProps.next()){
//				System.out.println(rsProps.getInt(1) + "\t\t" + rsProps.getString(2));
//			}
			
			if (rsProps != null) 
			{
			  rsProps.last();    // moves cursor to the last row
			  rsSize = rsProps.getRow(); // get row id 
			}
			
			rsProps.close();
			st.close();
		}catch(SQLException e){
			e.printStackTrace();
		}
		assertEquals(7, rsSize);
	}//end method	

	
}//end class
