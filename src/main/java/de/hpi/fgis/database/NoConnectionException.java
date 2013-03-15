package de.hpi.fgis.database;

public class NoConnectionException extends RuntimeException {
	private static final long serialVersionUID = 3987045586028294413L;

	public NoConnectionException() {
	}

	public NoConnectionException(String msg) {
		super(msg);
	}

	public NoConnectionException(Throwable e) {
		super(e);
	}

	public NoConnectionException(String msg, Throwable e) {
		super(msg, e);
	}

}
