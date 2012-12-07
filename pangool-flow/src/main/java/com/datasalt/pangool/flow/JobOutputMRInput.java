package com.datasalt.pangool.flow;

public class JobOutputMRInput implements MRInput {

	String jobName;
	String name;
	
	public JobOutputMRInput(String jobName) {
		this(jobName, "output");
	}
	
	public JobOutputMRInput(String jobName, String name) {
		this.jobName = jobName;
		this.name = name;
	}
	
	@Override
	public String getId() {
		return jobName + "." + name;
	}
}
