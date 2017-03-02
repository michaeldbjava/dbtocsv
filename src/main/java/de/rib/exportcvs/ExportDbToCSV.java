package de.rib.exportcvs;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import com.mysql.jdbc.ResultSetMetaData;

public class ExportDbToCSV {

	public ExportDbToCSV() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String pathOfConfigFile = null;
		if (args.length == 1) {
			pathOfConfigFile = args[0];
		}

		boolean configFileExists = false;

		/*
		 * If first argument is an path to config file then use it. In other
		 * case use default file
		 */
		ConfigurationDBToCsv cDbToCvs = new ConfigurationDBToCsv();
		if (args.length != 0) {
			Path path = Paths.get(pathOfConfigFile);
			System.out.println(
					"****    Es wurde eine Konfigurationsdatei unter folgenden Pfad angegeben: " + pathOfConfigFile);
			configFileExists = Files.exists(path, new LinkOption[] { LinkOption.NOFOLLOW_LINKS });
			if (configFileExists == true) {
				System.out.println("****    Lese die Konfigurationsdatei Datei: " + pathOfConfigFile);
				cDbToCvs.readConfigFile(pathOfConfigFile);
			} else {
				// throw new Exception("Die unter dem Pfad angegebene
				// Konfigurationsdatei existiert nicht!");
				System.out.println("****    Die unter dem Pfad angegebene Konfigurationsdatei existiert nicht!");
			}
			// System.out.println("Config File Exists: " + configFileExists);
		} else {
			configFileExists = false;
			System.out.println(
					"****    Es wird die Standart Konfigurationsdatei verwendet.\nWenn Sie eine andere Konfigurationsdatei verwenden wollen, so uebergeben Sie bitte den Pfad zur Konfigurationsdatei als Parameter!");
			Path path = Paths.get("dbtocsv_config.xml");
			configFileExists = Files.exists(path, new LinkOption[] { LinkOption.NOFOLLOW_LINKS });
			if (configFileExists == true)
				cDbToCvs.readConfigFile("dbtocsv_config.xml");
			else
				System.out.println("****    Es existiert keine Standart Konfigurationsdatei!");
		}
		Map<String, String> errorMessages = CheckDBToCsvConfigurationInformation.validateInformation(cDbToCvs);

		try {
			if (errorMessages.size() == 0) {
				System.out.println("****    Der Inhalt der Konfigurationsdatei wurde erfolgreich überprüft.    ****");

				Connection con = cDbToCvs.getConnectionToDb();
				System.out.println("****    Die Verbindung zur Datenbank " + cDbToCvs.getDatabase() + " unter einem " + cDbToCvs.getDbtype() + "Datenbanksystem wurde hergestellt.");
				// Als erstes prüfen, ob bereits eine Ausgabedatei unter dem
				// Pfad
				// existiert
				Path pathOutputFile = Paths.get(cDbToCvs.getCsvfile());

				Statement statement = con.createStatement();

				ResultSet rs = statement.executeQuery(cDbToCvs.getSqlStatement());

				System.out.println("****    Die SQL Abfrage wurde erfolgreich durchgeführt!");
				// FileWriter fileWriter = new
				// FileWriter(cDbToCvs.getCsvfile());
				Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(cDbToCvs.getCsvfile()),
						cDbToCvs.getCsvfileEncoding()));

				// CSVFormat csvFormat =
				// CSVFormat.newFormat(cDbToCvs.getDelimeter()).withRecordSeparator("\n").withHeader(rs).RFC4180;

				CSVFormat csvFormat = CSVFormat.newFormat(cDbToCvs.getDelimeter()).withRecordSeparator("\n")
						.withHeader(rs);
				// CSVFormat csvFormat =
				// CSVFormat.newFormat(cDbToCvs.getDelimeter()).withRecordSeparator("\n").MYSQL;
				// CSVPrinter csvPrinter = new CSVPrinter(fileWriter,
				// csvFormat);
				// CSVFormat csvFormat = CSVFormat.MYSQL.withHeader(rs);
				CSVPrinter csvPrinter = new CSVPrinter(out, csvFormat);
				System.out.println("****    Schreibe die CSV Datei.");
				csvPrinter.printRecords(rs);

				out.flush();
				out.close();
				System.out.println(
						"****    Die CSV Datei + " + cDbToCvs.getCsvfile() + " wurde erfolgreich gespeichert.");

				/*
				 * Hier den Schalter der Konfigurationsdatei auslesen und
				 * auswerten.
				 */

				if (cDbToCvs.getAfterExportUpdate() != null && cDbToCvs.getAfterExportUpdate().equals("true")) {
					System.out.println("****   Start after-export-update activities!");
					java.sql.ResultSetMetaData rsmd = rs.getMetaData();

					String tableName = rsmd.getTableName(1);
					/* exported_date */

					int colCount = rsmd.getColumnCount();
					boolean exportedDateColumnExists = false;
					for (int i = 1; i < colCount + 1; i++) {
						String columnName = rsmd.getColumnName(i);
//						System.out.println(columnName);
						if (columnName != null && columnName.equals("exported_date")) {
							exportedDateColumnExists = true;
						}
					}

					String updateSQL = "update " + tableName + " set exported_date=now() where ";
					ResultSet rsPrimaryKey = con.getMetaData().getPrimaryKeys(null, null, tableName);
					String whereExpression = "";
					ArrayList<String> updateSQLList = new ArrayList<String>();
					rs.beforeFirst();
					while (rs.next()) {
						whereExpression = "";
						rsPrimaryKey.beforeFirst();
						while (rsPrimaryKey.next()) {
							boolean isNumber = false;
							String columnName = rsPrimaryKey.getString("COLUMN_NAME");
							DatabaseMetaData rbMD = con.getMetaData();
							ResultSet rsColumnMeta = rbMD.getColumns(null, null, tableName, columnName);
							// System.out.println("----------------");
							while (rsColumnMeta.next()) {
								int type = rsColumnMeta.getInt(5);
								if (type == Types.BIGINT || type == Types.BIT || type == Types.DECIMAL
										|| type == Types.DOUBLE || type == Types.FLOAT || type == Types.INTEGER
										|| type == Types.NUMERIC || type == Types.SMALLINT) {
									isNumber = true;
								}
							}
							// System.out.println("Primary Key: " +
							// columnName);
							if (rsPrimaryKey.isLast()) {
								if (isNumber == true)
									whereExpression = whereExpression + columnName + "=" + rs.getString(columnName);
								else if (isNumber == false)
									whereExpression = whereExpression + columnName + "='" + rs.getString(columnName)
											+ "'";

							} else {
								if (isNumber == true)
									whereExpression = whereExpression + columnName + "=" + rs.getString(columnName)
											+ " and ";
								else if (isNumber == false)
									whereExpression = whereExpression + columnName + "='" + rs.getString(columnName)
											+ "'" + " and ";
								;
							}
							// System.out.println("----------------");

						}
						whereExpression = whereExpression + ";";

						System.out.println("****    " + updateSQL + whereExpression);
						updateSQLList.add(updateSQL + whereExpression);

					}

					// System.out.println("Export Table Name: " +
					// tableName);

					Statement updateStatement = con.createStatement();

					for (String updateSQLValue : updateSQLList) {
						updateStatement.addBatch(updateSQLValue);
					}
					updateStatement.executeBatch();
					System.out.println("****    after-export-update wurde durchgeführt!");
				} else {
					System.out.println("****    No after-export-update activities!");
				}
				con.close();
			} else {
				Collection<String> messages = errorMessages.values();
				Iterator<String> itMessages = messages.iterator();
				while (itMessages.hasNext()) {
					System.out.println(itMessages.next());
				}

			}

		} catch (SQLException e) {
			System.out.println(e.getLocalizedMessage());

		} catch (IOException i) {
			i.printStackTrace();
		}

	}

}
