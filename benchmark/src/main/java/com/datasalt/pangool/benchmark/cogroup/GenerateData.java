package com.datasalt.pangool.benchmark.cogroup;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Generates data that can be used as input for {@link UrlResolution}, {@link MapRedUrlResolution},
 * {@link CascadingUrlResolution}, {@link CrunchUrlResolution}
 * <p>
 * The generated output will be two tabulated text files: one containing a URL mapping in the form of: {url
 * cannnonicalUrl} and the other one containing a register of URLs in the form of: {url timestamp ip}.
 */
public class GenerateData {

	final static int TIMEFRAME = 100000;

	public static void main(String[] args) throws IOException {
		if(args.length != 5) {
			System.err.println();
			System.err.println("Five arguments are needed.");
			System.err
			    .println("Usage: [out-url-map] [out-url-reg] [#number_of_cannonical_urls] [#number_of_urls_per_cannonical] [#number_of_timestamp_per_url].");
			System.err.println();
			System.err
			    .println("Example: url-map.txt url-reg.txt 3, 3, 5 -> Will generate a file url-map.txt with 3x3=9 urls mapping to 3 different cannonical urls.");
			System.err
			    .println("ill also generate a file url-reg.txt with 3x3x5 = 45 records out of 3 cannonical URLs mapped to 3 URLs each and mapped to 5 different timestamp each.");
			System.err.println();
			System.exit(-1);
		}
		BufferedWriter writerUrlMap = new BufferedWriter(new FileWriter(args[0]));
		BufferedWriter writerUrlReg = new BufferedWriter(new FileWriter(args[1]));

		final int nCannonicalUrls = Integer.parseInt(args[2]);
		final int nUrlsPerCannonical = Integer.parseInt(args[3]);
		final int nTimestampsPerUrl = Integer.parseInt(args[4]);

		for(int i = 0; i < nCannonicalUrls; i++) {
			String randomCannonicalUrl = "http://foo." + randomChar() + randomChar() + "." + randomChar() + randomChar()
			    + "." + randomChar() + randomChar() + ".cannonical";
			for(int j = 0; j < nUrlsPerCannonical; j++) {
				String randomUrl = "http://foo" + randomChar() + randomChar() + "." + randomChar() + randomChar() + "."
				    + randomChar() + randomChar();
				writerUrlMap.write(randomUrl + "\t" + randomCannonicalUrl + "\n");
				for(int k = 0; k < nTimestampsPerUrl; k++) {
					long randomDate = System.currentTimeMillis() - (int) (Math.random() * TIMEFRAME);
					writerUrlReg.write(randomUrl + "\t" + randomDate + "\tIP" + "\n");
				}
			}
		}

		writerUrlMap.close();
		writerUrlReg.close();
	}

	public static char randomChar() {
		return (char) ((int) (Math.random() * 26) + 'a');
	}
}
