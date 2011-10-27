package com.datasalt.pangolin.commons;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

/**
 * A class for managing data repositories in Hadoop. A repository is a collection of packages. A package is the base
 * place where the execution of Hadoop pipeline relies on. All the outputs of each single Hadoop pipeline execution are
 * written to same package.
 * 
 * Each execution of a Hadoop pipeline needs a different package. This class helps with the management of repositories,
 * by allowing creating new packages, and navigate over the existing ones.
 * 
 * The package name format is [package_prefix]-yyyy-MM-dd-HH-mm-ss-[static counter]. Each package has an associated
 * status.
 * 
 * @author ivan
 */
@SuppressWarnings({ "unchecked" })
public class RepoTool {

	Path basePath;
	FileSystem fs;
	String packagePrefix;
	SimpleDateFormat dateFormatter;

	// Counter for avoiding same name package in the same second
	static AtomicInteger count = new AtomicInteger();

	public final static String PACKAGE_STATUS_FILE = "package.status";

	public enum PackageStatus {
		NOT_DEFINED, FINISHED, FAILED
	}

	public FileSystem getFileSystem(){
		return fs;
	}
	
	public RepoTool(Path basePath, String packagePrefix, FileSystem fs) {
		this.basePath = basePath;
		this.fs = fs;
		this.packagePrefix = packagePrefix;

		dateFormatter = new SimpleDateFormat("'" + packagePrefix + "'-" + "yyyy-MM-dd-HH-mm-ss");
	}

	/**
	 * Returns a new package using the current date. The new package is created and marked status NOT_DEFINED
	 */
	public Path newPackage() throws IOException {
		Path p = new Path(basePath, dateFormatter.format(new Date()) + "-" + String.format("%05d", count.incrementAndGet()));
		fs.mkdirs(p);
		setStatus(fs, p, PackageStatus.NOT_DEFINED);
		return p;
	}

	protected Path[] getPackages(Predicate<FileStatus> filter) throws IOException {
		FileStatus[] lfs = fs.listStatus(basePath);
		if(lfs == null) {
			return new Path[0];
		}

		ArrayList<String> paths = new ArrayList<String>();
		for(int i = 0; i < lfs.length; i++) {
			FileStatus status = lfs[i];
			if(status.isDir()) {
				try {
					dateFormatter.parse(status.getPath().getName());

					if(!filter.apply(status)) {
						paths.add(status.getPath().toString());
					}

				} catch(ParseException e) {
					continue;
				}
			}
		}

		Collections.sort(paths, Collections.reverseOrder());

		Path[] ret = new Path[paths.size()];
		for(int i = 0; i < paths.size(); i++) {
			ret[i] = new Path(paths.get(i));
		}
		return ret;
	}

	/**
	 * Return the list of packages in this repository, sorted by date in reverse order.
	 */
	public Path[] getPackages() throws IOException {
		return getPackages(Predicates.<FileStatus> alwaysFalse());
	}

	/**
	 * Return the list of packages with the given status in this repository, sorted by date in reverse order.
	 */
	public Path[] getPackagesWithStatus(final Enum<?> status) throws IOException {
		return getPackages(new Predicate<FileStatus>() {

			@Override
			public boolean apply(FileStatus input) {
				try {
					return status != getStatus(fs, status.getClass(), input.getPath());
				} catch(IOException e) {
					new RuntimeException("Exception when retriving status from the package " + input.getPath().toString(), e);
					return true;
				}
			}

		});
	}

	/**
	 * Return the newest package with the given status. Null if no package at all.
	 */
	public Path getNewestPackageWithStatus(Enum<?> status) throws IOException {
		Path[] list = getPackagesWithStatus(status);
		if(list != null && list.length > 0) {
			return getPackagesWithStatus(status)[0];
		} else {
			return null;
		}
	}

	/**
	 * Sets the given status for the given package. A file with name status.getClass().getSimpleName() is created in the
	 * package folder to save the status.
	 */
	public static void setStatus(FileSystem fs, Path packagePath, Enum<?> status) throws IOException {
		HadoopUtils.stringToFile(fs, new Path(packagePath, status.getClass().getSimpleName()), status.toString());
	}

	/**
	 * Return the status for the given enumType in the given package. Null if not status is found.
	 */
	public static <T extends Enum<T>> T getStatus(FileSystem fs, Class<T> enumClass, Path packagePath) throws IOException {
		String status = HadoopUtils.fileToString(fs, new Path(packagePath, enumClass.getSimpleName()));
		if(status != null) {
			return Enum.valueOf(enumClass, status.trim());
		} else {
			return null;
		}
	}
}
