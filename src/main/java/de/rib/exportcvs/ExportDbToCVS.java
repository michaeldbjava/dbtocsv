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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class ExportDbToCVS {

	public ExportDbToCVS() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String pathOfConfigFile = null;
		if(args.length==1){
			pathOfConfigFile=args[0];
		}
			
		
		boolean configFileExists = false;
		/*
		 * If first argument is an path to config file then use it. In other
		 * case use default file
		 */
		if (args.length != 0) {
			Path path = Paths.get(pathOfConfigFile);
			System.out.println("Übergebener Pfad: " + pathOfConfigFile);
			configFileExists = Files.exists(path, new LinkOption[] { LinkOption.NOFOLLOW_LINKS });
			//System.out.println("Config File Exists: " + configFileExists);
		}
		else{
			configFileExists=false;
			System.out.println("Es wird die Standart Konfigurationsdatei verwendet.\nWenn Sie eine andere Konfigurationsdatei verwenden wollen, so uebergeben Sie bitte den Pfad zur Konfigurationsdatei als Parameter!");
		}
		ConfigurationDBToCvs cDbToCvs = new ConfigurationDBToCvs();
		if (configFileExists == true) {
			System.out.println("Lese Config Datei: " + pathOfConfigFile);
			cDbToCvs.readConfigFile(pathOfConfigFile);
		}
		else{
			cDbToCvs.readConfigFile("dbtocvs_config.xml");
		}
		Connection con = cDbToCvs.getConnectionToDb();
		try {
			System.out.println("Verbindung ist geschlossen: " + con.isClosed());
			Statement statement = con.createStatement();
			ResultSet rs = statement.executeQuery(cDbToCvs.getSqlStatement());
			int colCount = rs.getMetaData().getColumnCount();
			System.out.println("Spaltenanzahl des RS" + rs.getMetaData().getColumnCount());
			
			//FileWriter fileWriter = new FileWriter(cDbToCvs.getCsvfile());
			Writer out = new BufferedWriter(new OutputStreamWriter(
				    new FileOutputStream(cDbToCvs.getCsvfile()), cDbToCvs.getCsvfileEncoding()));
			
			//CSVFormat csvFormat = CSVFormat.newFormat(cDbToCvs.getDelimeter()).withRecordSeparator("\n").withHeader(rs).RFC4180;
			CSVFormat csvFormat = CSVFormat.newFormat(cDbToCvs.getDelimeter()).withRecordSeparator("\n").withHeader(rs);
			//CSVFormat csvFormat = CSVFormat.newFormat(cDbToCvs.getDelimeter()).withRecordSeparator("\n").MYSQL;
			//CSVPrinter csvPrinter = new CSVPrinter(fileWriter, csvFormat);
			//CSVFormat csvFormat = CSVFormat.MYSQL.withHeader(rs);
			CSVPrinter csvPrinter = new CSVPrinter(out, csvFormat);
			csvPrinter.printRecords(rs);
			
			out.flush();
			out.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

}
