package de.hpi.fgis.yql;

/**
 * Signals that an error has been reached unexpectedly while deserializing data.
 * 
 * @author tonigr
 * 
 */
public class DeserializationException extends Exception {
	private static final long serialVersionUID = -3327439388625802461L;

	public DeserializationException() {
		super();
	}

	public DeserializationException(String message, Throwable cause) {
		super(message, cause);
	}

	public DeserializationException(String message) {
		super(message);
	}

	public DeserializationException(Throwable cause) {
		super(cause);
	}

}
