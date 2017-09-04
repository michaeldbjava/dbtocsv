/**
 * 
 */
package de.rib.exportcvs;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import de.rib.errorhandling.DbToCsvErrorConfigFileErrorHandler;

/**
 * @author michael
 *
 */
public class ConfigurationDBToCsv {
	private String csvfile;
	private String csvfileEncoding;
	private String localhost;
	private String database;
	private String dbtype;
	private String port;
	private String user;
	private String password;
	private String sqlStatement;
	private char delimeter;
	private boolean overwrite = false;
	private Connection conToDb = null;
	private String afterExportUpdate;
	private String afterExportUpdateColumn;

	/**
	 * 
	 */
	public ConfigurationDBToCsv() {
		// TODO Auto-generated constructor stub
	}

	public String getCsvfile() {
		return csvfile;
	}

	public void setCsvfile(String csvfile) {
		this.csvfile = csvfile;
	}

	public String getLocalhost() {
		return localhost;
	}

	public void setLocalhost(String localhost) {
		this.localhost = localhost;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getDbtype() {
		return dbtype;
	}

	public void setDbtype(String dbtype) {
		this.dbtype = dbtype;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getSqlStatement() {
		return sqlStatement;
	}

	public void setSqlStatement(String sqlStatement) {
		this.sqlStatement = sqlStatement;
	}

	public char getDelimeter() {
		return delimeter;
	}

	public void setDelimeter(char delimeter) {
		this.delimeter = delimeter;
	}

	public String getCsvfileEncoding() {
		return csvfileEncoding;
	}

	public void setCsvfileEncoding(String csvfileEncoding) {
		this.csvfileEncoding = csvfileEncoding;
	}

	public boolean isOverwrite() {
		return overwrite;
	}

	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}

	public String getAfterExportUpdate() {
		return afterExportUpdate;
	}

	public void setAfterExportUpdate(String afterExportUpdate) {
		this.afterExportUpdate = afterExportUpdate;
	}

	
	public String getAfterExportUpdateColumn() {
		return afterExportUpdateColumn;
	}

	public void setAfterExportUpdateColumn(String afterExportUpdateColumn) {
		this.afterExportUpdateColumn = afterExportUpdateColumn;
	}

	public boolean readConfigFile(String pathXMLConfigFile) {
		try {
			File xmlFile = new File(pathXMLConfigFile);
			File xsdFile = new File("dbtocsv_config_schema.xsd");
			System.out.println("Schema Beschreibung für die Struktur der XML Konfigurationsdatei existiert" + xsdFile.exists());
			SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			Schema schema = schemaFactory.newSchema(xsdFile);

			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();

			dbFactory.setSchema(schema);
			dbFactory.setValidating(false);

			DocumentBuilder documentBuilder = dbFactory.newDocumentBuilder();
			documentBuilder.setErrorHandler(new DbToCsvErrorConfigFileErrorHandler());
			Document document = documentBuilder.parse(xmlFile);

			this.setCsvfile(getValueOfXMLNode(document, "filename"));
			this.setCsvfileEncoding(getValueOfXMLNode(document, "file_encoding"));
			this.setDelimeter(getValueOfXMLNode(document, "delimeter").charAt(0));
			this.setDbtype(getValueOfXMLNode(document, "dbtype"));
			this.setLocalhost(getValueOfXMLNode(document, "host"));
			this.setDatabase(getValueOfXMLNode(document, "database_name"));
			this.setUser(getValueOfXMLNode(document, "user"));
			this.setPassword(getValueOfXMLNode(document, "password"));
			this.setPort(getValueOfXMLNode(document, "port"));
			this.setSqlStatement(getValueOfXMLNode(document, "sql-query"));

			this.setOverwrite(Boolean.parseBoolean(getValueOfXMLNode(document, "overwrite")));

			NodeList nodeOfTag = document.getElementsByTagName("after-export-update2");
			if (nodeOfTag != null && nodeOfTag.getLength() != 0) {
				
//				System.out.println("Laenge der NodeList: " + nodeOfTag.getLength());
				Element elementValue1 = (Element)nodeOfTag.item(0);
				NodeList elementValueFlag = elementValue1.getElementsByTagName("after-export-update-on");
				System.out.println(elementValueFlag.item(0).getTextContent());
				NodeList elementValueColumn = elementValue1.getElementsByTagName("date-column-to-update");
				System.out.println(elementValueColumn.item(0).getTextContent());
				this.setAfterExportUpdate(elementValueFlag.item(0).getTextContent());
				this.setAfterExportUpdateColumn(elementValueColumn.item(0).getTextContent());
			}

		} catch (IOException ioe) {
			System.out.println("****    " + ioe.getLocalizedMessage());
		} catch (ParserConfigurationException pce) {
			System.out.println("****    " + pce.getLocalizedMessage());
		} catch (SAXException se) {
			System.out.println("****    Message: " + se.getLocalizedMessage());

		}

		return true;

	}

	

	private String getValueOfXMLNode(Document document, String xmlNode) {
		String xmlNodeValue = xmlNode;

		NodeList nodeOfTag = document.getElementsByTagName(xmlNodeValue);
		Element elementOfTag = (Element) nodeOfTag.item(0);
		String valueOfElement = null;
		if (elementOfTag.getFirstChild() != null) {
			valueOfElement = elementOfTag.getFirstChild().getTextContent();
			return valueOfElement;
		} else {
			return null;
		}

	}

}
