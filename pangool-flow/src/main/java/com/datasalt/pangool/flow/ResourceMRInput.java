package com.datasalt.pangool.flow;

public class ResourceMRInput implements MRInput {

	String resourceName;
	
	public ResourceMRInput(String resourceName) {
		this.resourceName = resourceName;
	}

	@Override
  public String getId() {
	  return resourceName;
  }
}
