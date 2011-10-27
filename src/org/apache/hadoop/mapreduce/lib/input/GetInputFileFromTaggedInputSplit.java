package org.apache.hadoop.mapreduce.lib.input;

import org.apache.hadoop.mapreduce.InputSplit;

public class GetInputFileFromTaggedInputSplit {

	public static String get(InputSplit iS) {
		if(iS instanceof TaggedInputSplit) {
			TaggedInputSplit t = (TaggedInputSplit) iS;
			FileSplit fS = (FileSplit) t.getInputSplit();
			return fS.getPath().toString();
		} else if(iS instanceof FileSplit) {
			FileSplit fS = (FileSplit) iS;
			return fS.getPath().toString();
		}
		return null;
	}
}
