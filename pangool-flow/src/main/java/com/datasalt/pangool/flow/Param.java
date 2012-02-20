package com.datasalt.pangool.flow;

@SuppressWarnings("rawtypes")
public class Param {
	
	final String name;
	final String help;
  final Class clazz;
	
	public Param(String name, Class clazz) {
		this(name, clazz, null);
	}

	public Param(String name, Class clazz, String help) {
		this.name = name;
		this.clazz = clazz;
		this.help = help;
	}

	public String getName() {
  	return name;
  }

	public Class getClazz() {
  	return clazz;
  }
	
	public String toString() {
		return name + ":" + clazz.getSimpleName() + (help == null ? "" : "(" + help + ")");
	}
}