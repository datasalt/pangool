package com.datasalt.pangool.flow;


import java.util.ArrayList;

@SuppressWarnings("serial")
public class Inputs extends ArrayList<String> {

	public Inputs(String... inputs) {
		super(inputs.length);
		for(String input: inputs) {
			add(input);
		}
	}
}
