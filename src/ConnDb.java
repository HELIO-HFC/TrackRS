import java.sql.*;
//import java.io.*;

public class ConnDb {

	public Connection conn;

	public void init(String tbsp, String host) {
		
		String user = "ctrl_hfc1";
		String pass = "hfc2010";
		String address = "jdbc:mysql://" + host + ":3306/" + tbsp;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("Driver is loaded");
		}
		catch(Exception e) {
			System.err.println("Driver loading failed");
			e.printStackTrace();
		}
		
		
		
		try {
			conn = DriverManager.getConnection(address,user,pass);
	
			System.out.println("Connected to " + tbsp + " on " + host);
			conn.setAutoCommit(false);
		} catch(SQLException sqle) {
			System.err.println("Connection error");
			System.err.println(sqle.getSQLState() + " : " + sqle);
			while( (sqle=sqle.getNextException()) != null ) {
			      System.err.println(sqle.getSQLState() + " : " + sqle);
			}
		}
	}
	
	public void commit() {
		try {
			conn.commit();
		} catch(SQLException sqle) {
			System.err.println("Connection error");
			System.err.println(sqle.getSQLState() + " : " + sqle);
			while( (sqle=sqle.getNextException()) != null ) {
			      System.err.println(sqle.getSQLState() + " : " + sqle);
			}
		}
	}
	
	public void rollback() {
		try {
			conn.rollback();
		} catch(SQLException sqle) {
			System.err.println("Connection error");
			System.err.println(sqle.getSQLState() + " : " + sqle);
			while( (sqle=sqle.getNextException()) != null ) {
			      System.err.println(sqle.getSQLState() + " : " + sqle);
			}
		}
	}

	public void ferme() {
		try {
			conn.close();
			System.out.println("Connection closed");
		} catch(SQLException sqle) {
			System.err.println("Connection error");
			System.err.println(sqle.getSQLState() + " : " + sqle);
			while( (sqle=sqle.getNextException()) != null ) {
			      System.err.println(sqle.getSQLState() + " : " + sqle);
			}
		}
	}
}
