package com.bigspark.cloudera.management.helpers;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.*;
import java.util.Properties;


public class ImpalaHelper {

	private final String connectionString;
	
	public ImpalaHelper(String connectionString) {
		this.connectionString = connectionString;
	}

	public void invalidateMetadata(String dbName, String tableName) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException {
		try(Connection conn = this.getConnection()) {
			invalidateMetadata(conn, dbName, tableName);
		}
	}

	public void invalidateMetadata(Connection conn, String dbName, String tableName) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException {
		int attempt = 0;
		Boolean success = false;
		while(!success || attempt <= 3) {
			try {
				attempt++;
				try(Statement stmnt = conn.createStatement()) {
					stmnt.setQueryTimeout(600);
					stmnt.execute(String.format("invalidate metadata %s.%s", dbName, tableName));
					stmnt.execute(String.format("refresh %s.%s", dbName, tableName));
					success = true;
				} 
			} catch(SQLException ex) {
				if(ex.getMessage().contains("was modified while operation was in progress, aborting execution")) {
					Thread.sleep(1000);
				} else {
					throw ex;
				}
			}
		}
	}

	public void execute(String sql) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException {
		try(Connection conn = this.getConnection()) {
			try(Statement stmnt = conn.createStatement()) {
				stmnt.setQueryTimeout(600);
				stmnt.execute(sql);
			}
		}
	}


	public Connection getConnection() throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException {
		UserGroupInformation ugi  = UserGroupInformation.getLoginUser();
		final Driver driver = (Driver) Class.forName("com.cloudera.impala.jdbc41.Driver").newInstance();
		final String connectionString = this.connectionString;
		
		return ugi.doAs(new PrivilegedExceptionAction<Connection>() {
			@Override
			public Connection run() throws Exception {
				return driver.connect(connectionString, new Properties());
			}
		});
	}
}
