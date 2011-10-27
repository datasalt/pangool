package com.datasalt.pangolin.commons.flow;


public interface Executable<ConfigData> {

	public void execute(ConfigData configData) throws Exception;
	
}
