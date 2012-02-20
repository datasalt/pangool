package com.datasalt.pangool.flow;


import java.util.ArrayList;

@SuppressWarnings("serial")
public class Params extends ArrayList<Param> {
	
	public Params(Param... config) {
		super(config.length);
		for(Param cfg: config) {
			add(cfg);
		}
	}
	
	public static Params NONE = new Params();
}
