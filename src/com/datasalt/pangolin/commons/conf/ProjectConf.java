package com.datasalt.pangolin.commons.conf;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

/**
 * Project configuration
 */
@Singleton
public class ProjectConf {

	private String projectName;

	/**
	 * Return the proyect name.
	 */
	public String getProjectName() {
  	return projectName;
  }

	@Inject
	public void setProjectName(@Named("project.name") String projectName) {
  	this.projectName = projectName;
  }
	
}
