package com.athena.imis.models;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;


public class Database {

	private String username=""; 
	private String password="";
	private String url ="";
	int batchSize =1000;


	private String dbName ="";


	public Database(String url , String dbName,String username, String password, int batchSize) {
		this.dbName = dbName;
		this.password = password;
		this.url = url;
		this.username = username;
		this.batchSize = batchSize;
		
	}
	
	public Database(String url , String dbName,String username, String password) {
		this.dbName = dbName;
		this.password = password;
		this.url = url;
		this.username = username;
		this.batchSize = 1000;
		
	}
	
	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public void setUsername(String username) {
		this.username = username;
	}


	public void setPassword(String password) {
		this.password = password;
	}


	public void setUrl(String url) {
		this.url = url;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public String getDbName() {
		return dbName;
	}

	
	
	public Connection getConnection(Connection conn)  {

		boolean toCreate = false;
		
		try{
			if (Objects.nonNull(conn)) {			
				if (!conn.isValid(0)){
					toCreate=true;
				}
			}else {
				toCreate=true;
			} 
			
			if (toCreate) {		
			
			Class.forName("org.postgresql.Driver");
			conn = DriverManager
					.getConnection("jdbc:postgresql://"+this.url+":5432/" + this.dbName.toLowerCase(), this.username, this.password);
			}
			
		} catch ( Exception e ) {
			System.err.println(e.getMessage());
		}

		return conn;

	}
	
	
	public Connection resetServerConnection()  {

		Connection conn=null;		
		try{
			
			Class.forName("org.postgresql.Driver");
			conn = DriverManager
					.getConnection("jdbc:postgresql://"+this.url+":5432/", this.username, this.password);
			
		} catch ( Exception e ) {
			System.err.println(e.getMessage());
		}

		return conn;

	}
	
	
}
