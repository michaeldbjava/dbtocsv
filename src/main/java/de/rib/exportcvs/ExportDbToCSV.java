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
		System.out.print("\033[H\033[2J");
		System.out.flush();
		ConfigurationDBToCsv cDbToCvs = new ConfigurationDBToCsv();
		System.out.println("********************************************");
		System.out.println("********************************************");
		System.out.println("****                      ******************");
		System.out.println("****    Starte Export!    ******************");
		System.out.println("****                      ******************");
		System.out.println("********************************************");
		System.out.println("********************************************");
		System.out.println("****    ");
		if (args.length != 0) {
			Path path = Paths.get(pathOfConfigFile);
			
			
			System.out.println(
					"****    1) Es wurde eine Konfigurationsdatei unter folgenden Pfad angegeben: \n****       " + pathOfConfigFile);
			System.out.println("****    ");
			configFileExists = Files.exists(path, new LinkOption[] { LinkOption.NOFOLLOW_LINKS });
			if (configFileExists == true) {
				System.out.println("****    2) Lese die Konfigurationsdatei Datei: " + pathOfConfigFile);
				System.out.println("****    ");
				cDbToCvs.readConfigFile(pathOfConfigFile);
			} else {
				// throw new Exception("Die unter dem Pfad angegebene
				// Konfigurationsdatei existiert nicht!");
				System.out.println("****    Die unter dem Pfad angegebene Konfigurationsdatei existiert nicht!");
				System.out.println("****    ");
				
			}
			// System.out.println("Config File Exists: " + configFileExists);
		} else {
			configFileExists = false;
			System.out.println(
					"****    1) Es wurde keine Konfigurationsdatei angegeben.\n****       Bitte geben Sie eine Konfigurationsdatei an!"
					+ "\n****       Uebergeben Sie bitte den Pfad zur Konfigurationsdatei als Parameter!"
					+ "\n****\n****       Der Aufruf des Programms muss wie folgt erfolgen: "
					+ "\n****\n****        "
					+ "\n****       java -jar dbtocsv.jar dbtocsv_config_xxx.xml"
					+ "\n****\n****        "
					+ "\n****       Lesen Sie bitte die beiligende Dokumentation!"
					+ "\n****\n****        "
					);
	
			
				System.out.println("****    2) Das Export Programm wird abgebrochen!" + "\n****\n****        ");
		}
		
		if(configFileExists==true){
		try {
			Map<String, String> errorMessages = CheckDBToCsvConfigurationInformation.validateInformation(cDbToCvs);
			if (errorMessages.size() == 0) {
				System.out.println("****    3) Der Inhalt der Konfigurationsdatei wurde erfolgreich überprüft.    ****");
				System.out.println("****    ");

				Connection con = cDbToCvs.getConnectionToDb();
				System.out.println("****    4) Die Verbindung zur Datenbank " + cDbToCvs.getDatabase() + "\n****       unter einem " + cDbToCvs.getDbtype() + "Datenbanksystem wurde hergestellt.");
				System.out.println("****    ");
				// Als erstes prüfen, ob bereits eine Ausgabedatei unter dem
				// Pfad
				// existiert
				Path pathOutputFile = Paths.get(cDbToCvs.getCsvfile());

				Statement statement = con.createStatement();

				ResultSet rs = statement.executeQuery(cDbToCvs.getSqlStatement());

				System.out.println("****    5) Die SQL Abfrage wurde erfolgreich durchgeführt!");
				System.out.println("****    ");
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
				System.out.println("****    6) Schreibe die CSV Datei.");
				System.out.println("****    ");
				csvPrinter.printRecords(rs);

				out.flush();
				out.close();
				System.out.println(
						"****    7) Die CSV Datei  \n****       " + cDbToCvs.getCsvfile() + "\n****       wurde erfolgreich gespeichert.");
				System.out.println("****    ");

				/*
				 * Hier den Schalter der Konfigurationsdatei auslesen und
				 * auswerten.
				 */

				if (cDbToCvs.getAfterExportUpdate() != null && cDbToCvs.getAfterExportUpdate().equals("true")) {
					System.out.println("****   8) Start after-export-update activities!");
					System.out.println("****    ");
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
					String updateSQL ="";
					if(cDbToCvs.getDbtype()!=null && (cDbToCvs.getDbtype().toLowerCase().equals("mysql") || cDbToCvs.getDbtype().toLowerCase().equals("postgresql"))){
						updateSQL = "update " + tableName + " set export_datum=now() where ";
					}
					else if (cDbToCvs.getDbtype()!=null && cDbToCvs.getDbtype().toLowerCase().equals("mssqlserver")){
						updateSQL = "update " + tableName + " set export_datum=sysdatetime() where ";
					}
					else if (cDbToCvs.getDbtype()!=null && cDbToCvs.getDbtype().toLowerCase().equals("sqlite")){
						updateSQL = "update " + tableName + " set export_datum=datetime('now') where ";
					}
					
					/*
					 * 
					 */
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

						System.out.println("****    9) Führe folgende Aktualisierung durch: " + updateSQL + whereExpression);
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
					System.out.println("****    8) No after-export-update activities!");
					System.out.println("****    ");
				}
				System.out.println("********************************************");
				System.out.println("****                      ******************");
				System.out.println("****    Ende Export!      ******************");
				System.out.println("****                      ******************");
				System.out.println("********************************************");
				System.out.println("********************************************");
				con.close();
			} else {
				Collection<String> messages = errorMessages.values();
				Iterator<String> itMessages = messages.iterator();
				while (itMessages.hasNext()) {
					System.out.println(itMessages.next());
				}

			}

		} catch (SQLException e) {
			System.out.println("****    Datenbankfehler: " + e.getMessage());

		} catch (IOException i) {
			System.out.println("****    Dateischreibfehler: " + i.getMessage());
		}
		}
		else{
			System.out.println("****    Die in der Konfigurationsdatei angegeben\n****    Informationen sind nicht valide!");
			System.out.println("****    ");
			System.out.println("********************************************");
			System.out.println("****                      ******************");
			System.out.println("****    Ende Export!      ******************");
			System.out.println("****                      ******************");
			System.out.println("********************************************");
			System.out.println("********************************************");
			
		}

	}

}
