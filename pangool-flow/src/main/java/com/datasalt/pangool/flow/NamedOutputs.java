package com.datasalt.pangool.flow;


import java.util.ArrayList;

@SuppressWarnings("serial")
public class NamedOutputs extends ArrayList<String> {

	public final static String OUTPUT = "output";
	
	public NamedOutputs(String... outputs) {
		super(outputs.length);
		for(String output: outputs) {
			if(output.equals(OUTPUT)) {
				throw new IllegalArgumentException("can't use reserved output name: [" + output + "] - reserved for main output.");
			}
			add(output);
		}
	}
	
	public static NamedOutputs NONE = new NamedOutputs();
}
