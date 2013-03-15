package de.hpi.fgis;

import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.Connection.Response;

public class HTMLLoader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String url;
		if(args.length>0) {
			url = args[0];
		} else {
			throw new IllegalArgumentException("Please specify an URL o load!");
		}
		Response res = Jsoup.connect(url).userAgent("Mozilla/7.0 (X11; U; Linux i2058;) Gecko/Ubuntu/8.04 (hardy) Firefox/4.0.1 (alpha)").timeout(1500).execute();
		
		System.out.println(res.headers());
	}

}
