package de.rib.exportcvs;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class CheckDBToCsvConfigurationInformation {
	private ConfigurationDBToCsv cDbToCsv;

	/*
	 * vielleicht sollte ich hier nur den Dateinamen der Konfigurationsdatei
	 * übergeben, um auch deren Existenz direkt mit zu prüfen
	 */
	public CheckDBToCsvConfigurationInformation(ConfigurationDBToCsv cDbToCsv) {
		this.cDbToCsv = cDbToCsv;
		// TODO Auto-generated constructor stub
	}

	public static Map<String, String> validateInformation(ConfigurationDBToCsv cDbToCsv) {
		Map<String, String> errorMessageList = new HashMap<String, String>();
		boolean statusOfAllValidationInformation = false;

		/* At second check csv file information */
		String csvFileName = cDbToCsv.getCsvfile();
		boolean overwriteCsvFile = cDbToCsv.isOverwrite();
		String csvFileEncoding = cDbToCsv.getCsvfileEncoding();
		String csvDelimeter = Character.toString(cDbToCsv.getDelimeter());

		// Check csv file name
		if (!(csvFileName != null && !csvFileName.equals("") && csvFileName.endsWith(".csv")
				&& csvFileName.length() > 5)) {
			errorMessageList.put("csvFileName",
					"****    Der angegebene Name für die CSV Datei ist ungültig. Der Name der CSV Datei muss mit .csv enden!");
		}

		// Check existence of csv file and overwrite information
		Path pathOutputFile = Paths.get(cDbToCsv.getCsvfile());

		boolean outputfileExists = Files.exists(pathOutputFile, new LinkOption[] { LinkOption.NOFOLLOW_LINKS });
		if (outputfileExists && overwriteCsvFile == false) {
			errorMessageList.put("csvFileExist",
					"****    Die CSV Datei existiert bereits. Bitte konfigurieren Sie die Überschreibefunktion!");
		}

		// Check charset
		if (csvFileEncoding != null && csvFileEncoding.length() != 0) {
			if (Charset.availableCharsets().keySet().contains(csvFileEncoding)) {
			} else {
				errorMessageList.put("charset",
						"****    Der von Ihnen angegebene Zeichensatz, ist nicht auf Ihrem System verfügbar.");
			}
		} else {
			errorMessageList.put("charset2", "****    Bitte geben Sie einen Zeichensatz an!");
		}

		/* At third check Connection Information to database */
		String dbType = cDbToCsv.getDbtype();
		if (dbType != null && (dbType.toLowerCase().equals("mysql") || dbType.toLowerCase().equals("postgresql")
				|| dbType.toLowerCase().equals("mssqlserver") || dbType.toLowerCase().equals("sqlite"))) {

		} else {
			errorMessageList.put("database",
					"****    Bitte geben Sie ein Datenbanksystem (mysql, postgresql,mssqlserver, sqlite) an!");
		}
		String host = cDbToCsv.getLocalhost();
		String database = cDbToCsv.getDatabase();
		String port = cDbToCsv.getPort();
		String user = cDbToCsv.getUser();
		String password = cDbToCsv.getPassword();
		String sqlStatement = cDbToCsv.getSqlStatement();

		if (dbType.equals("mysql")) {

			try {
				Connection con = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + database + "?"
						+ "user=" + user + "&password=" + password);
				if (con.isValid(1000)) {
					con.close();

				} else {
					errorMessageList.put("database",
							"****    Es konnte keine Verbindung mit dem MySQL Datenbanksystem hergestellt werden!");
					errorMessageList.put("database2", "****    Bitte überprüfen Sie die Verbindungsparameter!!");
				}

			} catch (SQLException ex) {
				// handle any errors
				errorMessageList.put("database_exc0", "****    Es ist ein Fehler aufgetreten!");
				errorMessageList.put("database_exc1", "****    " + ex.getMessage());
				errorMessageList.put("database_exc2", "****    " + ex.getSQLState());
				errorMessageList.put("database_exc3", "****    " + ex.getErrorCode());

			}
		}

		if (dbType.equals("sqlite")) {

			try {
				Connection con = DriverManager.getConnection("jdbc:sqlite://" + database);
				if (con.isValid(1000)) {
					con.close();

				} else {
					errorMessageList.put("database",
							"****    Es konnte keine Verbindung mit dem SQLite Datenbanksystem hergestellt werden!");
					errorMessageList.put("database2", "****    Bitte überprüfen Sie die Verbindungsparameter!!");
				}

			} catch (SQLException ex) {
				// handle any errors
				errorMessageList.put("database_exc0", "****    Es ist ein Fehler aufgetreten!");
				errorMessageList.put("database_exc1", "****    " + ex.getMessage());
				errorMessageList.put("database_exc2", "****    " + ex.getSQLState());
				errorMessageList.put("database_exc3", "****    " + ex.getErrorCode());
			}
		}

		if (sqlStatement != null && !sqlStatement.equals("")) {
			if (!(sqlStatement.toLowerCase().matches(".*delete.*") || sqlStatement.toLowerCase().matches(".*truncate.*")
					|| sqlStatement.toLowerCase().matches(".*update.*")
					|| sqlStatement.toLowerCase().matches(".*insert.*"))) {

			} else {
				errorMessageList.put("forbiddenStatement",
						"****    Delete, Truncate, Update und Insert Anweisungen sind nicht zulaessig!");

			}
		}
		else{
				errorMessageList.put("noSqlStatement",
					"****    Bitte geben Sie eine gültige SQL Anweisung in der Konfigurationsdatei an!");
		}
		/* At fourth check after export activities */
		String afterExportUpdate = cDbToCsv.getAfterExportUpdate();
		return errorMessageList;

	}

}
