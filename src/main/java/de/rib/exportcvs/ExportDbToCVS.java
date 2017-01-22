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
		ConfigurationDBToCvs cDbToCvs = new ConfigurationDBToCvs();
		if (args.length != 0) {
			Path path = Paths.get(pathOfConfigFile);
			System.out.println("Es wurde eine Konfigurationsdatei unter folgenden Pfad angegeben: " + pathOfConfigFile);
			configFileExists = Files.exists(path, new LinkOption[] { LinkOption.NOFOLLOW_LINKS });
			if(configFileExists==true){
				System.out.println("Lese Config Datei: " + pathOfConfigFile);
				cDbToCvs.readConfigFile(pathOfConfigFile);
			}
			else{
				//throw new Exception("Die unter dem Pfad angegebene Konfigurationsdatei existiert nicht!");
				System.out.println("Die unter dem Pfad angegebene Konfigurationsdatei existiert nicht!");
			}
			//System.out.println("Config File Exists: " + configFileExists);
		}
		else{
			configFileExists=false;
			System.out.println("Es wird die Standart Konfigurationsdatei verwendet.\nWenn Sie eine andere Konfigurationsdatei verwenden wollen, so uebergeben Sie bitte den Pfad zur Konfigurationsdatei als Parameter!");
			Path path = Paths.get("dbtocvs_config.xml");
			configFileExists = Files.exists(path, new LinkOption[] { LinkOption.NOFOLLOW_LINKS });
			if(configFileExists==true)
				cDbToCvs.readConfigFile("dbtocvs_config.xml");
			else
				System.out.println("Die Standart Konfigurationsdatei existiert nicht");
		}
		
		
		
		
		

		Connection con = cDbToCvs.getConnectionToDb();
		try {
			
			Statement statement = con.createStatement();
			ResultSet rs = statement.executeQuery(cDbToCvs.getSqlStatement());
			System.out.println("Die SQL Abfrage wurde erfolgreich durchgef�hrt!");
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
			System.out.println("Die CSV Datei wurde erfolgreich als Datei gepseichert.");
		} catch (SQLException e) {
			System.out.println("Die SQL Abfrage hat einen Fehler verursucht.");
			System.out.println(e.getLocalizedMessage());
			
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

}
