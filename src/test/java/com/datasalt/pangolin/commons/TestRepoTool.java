package com.datasalt.pangolin.commons;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.datasalt.pangolin.commons.HadoopUtils;
import com.datasalt.pangolin.commons.RepoTool;
import com.datasalt.pangolin.commons.RepoTool.PackageStatus;
import com.datasalt.pangolin.commons.test.BaseTest;

public class TestRepoTool extends BaseTest {

	@Test
	public void test() throws IOException {
		FileSystem fs = FileSystem.getLocal(pisaeConfiguration.create());
		
		Path repo = new Path("repoTest87463829");
		HadoopUtils.deleteIfExists(fs, repo);

		RepoTool tool = new RepoTool(repo, "pkg", fs);
		
		assertNull(tool.getNewestPackageWithStatus(PackageStatus.NOT_DEFINED));
		
		Path pkg1 = tool.newPackage();
		assertEquals("pkg", pkg1.getName().substring(0,3));
		
		assertEquals(pkg1.makeQualified(fs), tool.getNewestPackageWithStatus(PackageStatus.NOT_DEFINED));
		
		Path pkg2 = tool.newPackage();
		assertEquals(pkg2.makeQualified(fs), tool.getNewestPackageWithStatus(PackageStatus.NOT_DEFINED));
		
		assertEquals(2, tool.getPackages().length);
		
		RepoTool.setStatus(fs, pkg2, PackageStatus.FINISHED);
		assertEquals(pkg2.makeQualified(fs), tool.getNewestPackageWithStatus(PackageStatus.FINISHED));
		
		HadoopUtils.deleteIfExists(fs, repo);
	}
	
}
