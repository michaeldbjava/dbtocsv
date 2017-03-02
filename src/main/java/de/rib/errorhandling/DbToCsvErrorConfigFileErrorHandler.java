/**
 * 
 */
package de.rib.errorhandling;

import org.xml.sax.ErrorHandler;

import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * @author michael
 *
 */
public class DbToCsvErrorConfigFileErrorHandler implements ErrorHandler  {

	/**
	 * 
	 */
	public DbToCsvErrorConfigFileErrorHandler() {
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see org.xml.sax.ErrorHandler#warning(org.xml.sax.SAXParseException)
	 */
	public void warning(SAXParseException exception) throws SAXException {
		// TODO Auto-generated method stub
		System.out.println("****    Warnung Parse Vorgang Konfigurationsdatei " + "\n**** Zeile:    "
				+ exception.getLineNumber() + "\n**** URI: "
				+ exception.getSystemId() + "\n**** Nachricht: "
				+ exception.getMessage()
				);
		throw new SAXException("****    Beim Parse Vorgang ist eine Warnung aufgetreten!");
	}

	/* (non-Javadoc)
	 * @see org.xml.sax.ErrorHandler#error(org.xml.sax.SAXParseException)
	 */
	public void error(SAXParseException exception) throws SAXException {
		// TODO Auto-generated method stub
		System.out.println("****    Fehler Parse Vorgang Konfigurationsdatei " + "\n**** Zeile:    " + exception.getLineNumber()
				 + "\n****    URI: " + exception.getSystemId()
				 + "\n****    Nachricht: "+ exception.getMessage()
				
				);
		throw new SAXException("****    Beim Parse Vorgang ist eine Fehler aufgetreten!");

	}

	/* (non-Javadoc)
	 * @see org.xml.sax.ErrorHandler#fatalError(org.xml.sax.SAXParseException)
	 */
	public void fatalError(SAXParseException exception) throws SAXException {
		// TODO Auto-generated method stub
		System.out.println("****    Fataler Fehler Parse Vorgang Konfigurationsdatei " + "\n**** Zeile:    "
				+ exception.getLineNumber() + "\n**** URI: "
				+ exception.getSystemId() + "\n**** Nachricht: "
				+ exception.getMessage()
				);
		throw new SAXException("****    Beim Parse Vorgang ist eine fataler Fehler aufgetreten!");

	}

}
