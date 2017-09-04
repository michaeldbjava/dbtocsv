package de.rib.exportcvs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {
	

	public static Connection createConnectionToDb(String dbType,String localhost,String port,String database,String user,String password) {
		Connection conToDb=null;
		/* Auch hier redundaten Kode neutralisieren */
		if (dbType.equals("mysql")) {

			try {
				conToDb = DriverManager.getConnection("jdbc:mysql://" + localhost + ":" + port + "/"
						+ database + "?" + "user=" + user + "&password=" + password);

			} catch (SQLException ex) {
				// handle any errors
				System.out.println("****    Beim Versuch eine Verbindung mit der Datenbank herzustellen, ist ein Fehler aufgetreten!");
				System.out.println("****    SQLException: " + ex.getMessage());
				System.out.println("****    SQLState: " + ex.getSQLState());
				System.out.println("****    VendorError: " + ex.getErrorCode());
			}
			return conToDb;
		}

		if (dbType.equals("sqlite")) {

			try {
				conToDb = DriverManager.getConnection("jdbc:sqlite://" + database);

			} catch (SQLException ex) {
				// handle any errors
				System.out.println("****    Beim Versuch eine Verbindung mit der Datenbank herzustellen, ist ein Fehler aufgetreten!");
				System.out.println("SQLException: " + ex.getMessage());
				System.out.println("SQLState: " + ex.getSQLState());
				System.out.println("VendorError: " + ex.getErrorCode());
			}
			return conToDb;
		}

		if (dbType.equals("postgresql")) {
			try {
				conToDb = DriverManager.getConnection(
						"jdbc:postgresql://" + localhost + ":" + port + "/" + database,
						user, password);

			} catch (SQLException ex) {
				// handle any errors
				System.out.println("****    Beim Versuch eine Verbindung mit der Datenbank herzustellen, ist ein Fehler aufgetreten!");
				System.out.println("****    SQLException: " + ex.getMessage());
				System.out.println("****    SQLState: " + ex.getSQLState());
				System.out.println("****    VendorError: " + ex.getErrorCode());
			}
			return conToDb;

		}

		if (dbType.equals("mssqlserver")) {
			try {
				conToDb = DriverManager.getConnection("jdbc:sqlserver://" + localhost + ":" + port
						+ ";databaseName=" + database + ";user=" + user + ";password="
						+ password + ";");

			} catch (SQLException ex) {
				// handle any errors
				System.out.println("****    SQLException: " + ex.getMessage());
				System.out.println("****    SQLState: " + ex.getSQLState());
				System.out.println("****    VendorError: " + ex.getErrorCode());
			}
			return conToDb;

		}

		return null;

	}

	
	
}
