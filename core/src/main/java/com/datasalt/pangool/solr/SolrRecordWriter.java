/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.solr;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Jdk14Logger;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;

import com.datasalt.pangool.io.ITuple;

/**
 * Instantiate a record writer that will build a Solr index.
 * 
 * A zip file containing the solr config and additional libraries is expected to be passed via the distributed cache.
 * The incoming written records are converted via the specified document converter, and written to the index in batches.
 * When the job is done, the close copies the index to the destination output file system.
 * <p>
 * <b>This class has been copied from SOLR-1301 patch although it might be slightly different from it.</b>
 * <p>
 */
public class SolrRecordWriter extends RecordWriter<ITuple, NullWritable> {
	static final Log LOG = LogFactory.getLog(SolrRecordWriter.class);

	public final static List<String> allowedConfigDirectories = new ArrayList<String>(Arrays.asList(new String[] {
	    "conf", "lib" }));

	public final static Set<String> requiredConfigDirectories = new HashSet<String>();
	static {
		requiredConfigDirectories.add("conf");
	}

	/**
	 * Return the list of directories names that may be included in the configuration data passed to the tasks.
	 * 
	 * @return an UnmodifiableList of directory names
	 */
	public static List<String> getAllowedConfigDirectories() {
		return Collections.unmodifiableList(allowedConfigDirectories);
	}

	/**
	 * check if the passed in directory is required to be present in the configuration data set.
	 * 
	 * @param directory
	 *          The directory to check
	 * @return true if the directory is required.
	 */
	public static boolean isRequiredConfigDirectory(final String directory) {
		return requiredConfigDirectories.contains(directory);
	}

	private TupleDocumentConverter converter;
	private EmbeddedSolrServer solr;
	private SolrCore core;
	private FileSystem fs;

	private int batchSize;

	/** The path that the final index will be written to */
	private Path perm;
	/** The location in a local temporary directory that the index is built in. */
	private Path local;
	/** The directory that the configuration zip file was unpacked into. */
	private Path solrHome = null;

	private static AtomicLong sequence = new AtomicLong(0);

	/**
	 * If true, create a zip file of the completed index in the final storage location A .zip will be appended to the
	 * final output name if it is not already present.
	 */
	private boolean outputZipFile = false;
	private Configuration conf;
	HeartBeater heartBeater = null;
	private BatchWriter batchWriter = null;

	private String localSolrHome;
	private String zipName;

	@SuppressWarnings("rawtypes")
	private static HashMap<TaskID, Reducer.Context> contextMap = new HashMap<TaskID, Reducer.Context>();

	protected boolean isClosing() {
		return closing;
	}

	protected void setClosing(boolean closing) {
		this.closing = closing;
	}

	/** If true, writes will throw an exception */
	volatile boolean closing = false;

	private String getOutFileName(TaskAttemptContext context, String prefix) {
		TaskID taskId = context.getTaskAttemptID().getTaskID();
		int partition = taskId.getId();
		NumberFormat nf = NumberFormat.getInstance();
		nf.setMinimumIntegerDigits(5);
		nf.setGroupingUsed(false);
		StringBuilder result = new StringBuilder();
		result.append(prefix);
		result.append("-");
		result.append(nf.format(partition));
		return result.toString();
	}

	public SolrRecordWriter(int batchSize, boolean outputZipFile, int threadCount, int queueSize, String localSolrHome,
	    String zipName, TupleDocumentConverter converter, TaskAttemptContext context) {
		this.localSolrHome = localSolrHome;
		this.zipName = zipName;
		conf = context.getConfiguration();
		this.batchSize = batchSize;

		setLogLevel("org.apache.solr.core", "WARN");
		setLogLevel("org.apache.solr.update", "WARN");
		Logger.getLogger("org.apache.solr.core").setLevel(Level.WARN);
		Logger.getLogger("org.apache.solr.update").setLevel(Level.WARN);
		java.util.logging.Logger.getLogger("org.apache.solr.core").setLevel(java.util.logging.Level.WARNING);
		java.util.logging.Logger.getLogger("org.apache.solr.update").setLevel(java.util.logging.Level.WARNING);

		setLogLevel("org.apache.solr", "WARN");
		Logger.getLogger("org.apache.solr").setLevel(Level.WARN);
		java.util.logging.Logger.getLogger("org.apache.solr").setLevel(java.util.logging.Level.WARNING);

		heartBeater = new HeartBeater(context);
		try {
			heartBeater.needHeartBeat();
			/** The actual file in hdfs that holds the configuration. */
			this.outputZipFile = outputZipFile;

			this.fs = FileSystem.get(conf);
			perm = new Path(FileOutputFormat.getOutputPath(context), getOutFileName(context, "part"));

			// Make a task unique name that contains the actual index output name to
			// make debugging simpler
			// Note: if using JVM reuse, the sequence number will not be reset for a
			// new task using the jvm

			Path temp = conf.getLocalPath("mapred.local.dir",
			    "solr_" + conf.get("mapred.task.id") + '.' + sequence.incrementAndGet());

			if(outputZipFile && !perm.getName().endsWith(".zip")) {
				perm = perm.suffix(".zip");
			}
			fs.delete(temp, true); // delete old, if any
			fs.delete(perm, true); // delete old, if any
			local = fs.startLocalOutput(perm, temp);

			solrHome = findSolrConfig(conf);

			// }
			// Verify that the solr home has a conf and lib directory
			if(solrHome == null) {
				throw new IOException("Unable to find solr home setting");
			}

			// Setup a solr instance that we can batch writes to
			LOG.info("SolrHome: " + solrHome.toUri());
			String dataDir = new File(local.toString(), "data").getAbsoluteFile().toString();
			// copy the schema to the conf dir
			File confDir = new File(local.toString(), "conf");
			confDir.mkdirs();
			
			File unpackedSolrHome = new File(solrHome.toString());
			FileUtils.copyDirectory(new File(unpackedSolrHome, "conf"), confDir);

			Properties props = new Properties();
			props.setProperty("solr.data.dir", dataDir);
			props.setProperty("solr.home", solrHome.toString());
			SolrResourceLoader loader = new SolrResourceLoader(solrHome.toString(), null, props);
			LOG.info(String
			    .format(
			        "Constructed instance information solr.home %s (%s), instance dir %s, conf dir %s, writing index to temporary directory %s, with permdir %s",
			        solrHome, solrHome.toUri(), loader.getInstanceDir(), loader.getConfigDir(), dataDir, perm));
			CoreContainer container = new CoreContainer(loader);
			CoreDescriptor descr = new CoreDescriptor(container, "core1", solrHome.toString());
			descr.setDataDir(dataDir);
			descr.setCoreProperties(props);
			core = container.create(descr);
			container.register(core, false);
			solr = new EmbeddedSolrServer(container, "core1");
			batchWriter = new BatchWriter(solr, batchSize, context.getTaskAttemptID().getTaskID(), threadCount, queueSize);

			this.converter = converter;
		} catch(Exception e) {
			e.printStackTrace();
			LOG.error(e);
			throw new IllegalStateException(String.format("Failed to initialize record writer for %s, %s",
			    context.getJobName(), conf.get("mapred.task.id")), e);
		} finally {
			heartBeater.cancelHeartBeat();
		}
	}

	public static void incrementCounter(TaskID taskId, String groupName, String counterName, long incr) {
		@SuppressWarnings("rawtypes")
		Reducer.Context context = contextMap.get(taskId);
		if(context != null) {
			context.getCounter(groupName, counterName).increment(incr);
		}
	}

	public static void addReducerContext(@SuppressWarnings("rawtypes") Reducer.Context context) {
		TaskID taskID = context.getTaskAttemptID().getTaskID();
		if(contextMap.get(taskID) == null) {
			contextMap.put(taskID, context);
		}
	}

	private Path findSolrConfig(Configuration conf) throws IOException {
		Path solrHome = null;

		// we added these lines to make this patch work on Hadoop 0.20.2
		FileSystem localFs = FileSystem.getLocal(conf);
		if(FileSystem.get(conf).equals(localFs)) {
			return new Path(localSolrHome);
		}
		// end-of-addition
		Path[] localArchives = DistributedCache.getLocalCacheArchives(conf);

		if(localArchives.length == 0) {
			throw new IOException(String.format("No local cache archives, where is %s", zipName));
		}
		for(Path unpackedDir : localArchives) {
			// Only logged if debugging
			if(LOG.isDebugEnabled()) {
				LOG.debug(String.format("Examining unpack directory %s for %s", unpackedDir, zipName));

				ProcessBuilder lsCmd = new ProcessBuilder(new String[] { "/bin/ls", "-lR", unpackedDir.toString() });
				lsCmd.redirectErrorStream();
				Process ls = lsCmd.start();
				try {
					byte[] buf = new byte[16 * 1024];
					InputStream all = ls.getInputStream();
					int count;
					while((count = all.read(buf)) > 0) {
						System.err.write(buf, 0, count);
					}
				} catch(IOException ignore) {
				}
				System.err.format("Exit value is %d%n", ls.exitValue());
			}
			if(unpackedDir.getName().equals(zipName)) {

				solrHome = unpackedDir;
				break;
			}
		}
		return solrHome;
	}

	Collection<SolrInputDocument> batch = new ArrayList<SolrInputDocument>();

	/**
	 * Write a record. This method accumulates records in to a batch, and when {@link #batchSize} items are present
	 * flushes it to the indexer. The writes can take a substantial amount of time, depending on {@link #batchSize}. If
	 * there is heavy disk contention the writes may take more than the 600 second default timeout.
	 */
	@Override
	public void write(ITuple key, NullWritable value) throws IOException {
		if(isClosing()) {
			throw new IOException("Index is already closing");
		}
		heartBeater.needHeartBeat();
		try {
			try {
				batch.add(converter.convert(key, value));
				if(batch.size() > batchSize) {
					batchWriter.queueBatch(batch);
					batch.clear();
				}
			} catch(SolrServerException e) {
				throw new IOException(e);
			}
		} finally {
			heartBeater.cancelHeartBeat();
		}

	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		if(context != null) {
			heartBeater.setProgress(context);
		}
		try {
			if(batch.size() > 0) {
				batchWriter.queueBatch(batch);
				batch.clear();
			}
			heartBeater.needHeartBeat();
			batchWriter.close(context, core);
			if(outputZipFile) {
				context.setStatus("Writing Zip");
				packZipFile(); // Written to the perm location
			} else {
				context.setStatus("Copying Index");
				fs.completeLocalOutput(perm, local); // copy to dfs
			}
		} catch(Exception e) {
			if(e instanceof IOException) {
				throw (IOException) e;
			}
			throw new IOException(e);
		} finally {
			heartBeater.cancelHeartBeat();
			File tempFile = new File(local.toString());
			if(tempFile.exists()) {
				FileUtils.forceDelete(new File(local.toString()));
			}
		}

		context.setStatus("Done");
	}

	private void packZipFile() throws IOException {
		FSDataOutputStream out = null;
		ZipOutputStream zos = null;
		int zipCount = 0;
		LOG.info("Packing zip file for " + perm);
		try {
			out = fs.create(perm, false);
			zos = new ZipOutputStream(out);

			String name = perm.getName().replaceAll(".zip$", "");
			LOG.info("adding index directory" + local);
			zipCount = zipDirectory(conf, zos, name, local.toString(), local);
		} catch(Throwable ohFoo) {
			LOG.error("packZipFile exception", ohFoo);
			if(ohFoo instanceof RuntimeException) {
				throw (RuntimeException) ohFoo;
			}
			if(ohFoo instanceof IOException) {
				throw (IOException) ohFoo;
			}
			throw new IOException(ohFoo);

		} finally {
			if(zos != null) {
				if(zipCount == 0) { // If no entries were written, only close out, as
					                  // the zip will throw an error
					LOG.error("No entries written to zip file " + perm);
					fs.delete(perm, false);
					// out.close();
				} else {
					LOG.info(String.format("Wrote %d items to %s for %s", zipCount, perm, local));
					zos.close();
				}
			}
		}
	}

	/**
	 * Write a file to a zip output stream, removing leading path name components from the actual file name when creating
	 * the zip file entry.
	 * 
	 * The entry placed in the zip file is <code>baseName</code>/ <code>relativePath</code>, where
	 * <code>relativePath</code> is constructed by removing a leading <code>root</code> from the path for
	 * <code>itemToZip</code>.
	 * 
	 * If <code>itemToZip</code> is an empty directory, it is ignored. If <code>itemToZip</code> is a directory, the
	 * contents of the directory are added recursively.
	 * 
	 * @param zos
	 *          The zip output stream
	 * @param baseName
	 *          The base name to use for the file name entry in the zip file
	 * @param root
	 *          The path to remove from <code>itemToZip</code> to make a relative path name
	 * @param itemToZip
	 *          The path to the file to be added to the zip file
	 * @return the number of entries added
	 * @throws IOException
	 */
	static public int zipDirectory(final Configuration conf, final ZipOutputStream zos, final String baseName,
	    final String root, final Path itemToZip) throws IOException {
		LOG.info(String.format("zipDirectory: %s %s %s", baseName, root, itemToZip));
		LocalFileSystem localFs = FileSystem.getLocal(conf);
		int count = 0;

		final FileStatus itemStatus = localFs.getFileStatus(itemToZip);
		if(itemStatus.isDir()) {
			final FileStatus[] statai = localFs.listStatus(itemToZip);

			// Add a directory entry to the zip file
			final String zipDirName = relativePathForZipEntry(itemToZip.toUri().getPath(), baseName, root);
			final ZipEntry dirZipEntry = new ZipEntry(zipDirName + Path.SEPARATOR_CHAR);
			LOG.info(String.format("Adding directory %s to zip", zipDirName));
			zos.putNextEntry(dirZipEntry);
			zos.closeEntry();
			count++;

			if(statai == null || statai.length == 0) {
				LOG.info(String.format("Skipping empty directory %s", itemToZip));
				return count;
			}
			for(FileStatus status : statai) {
				count += zipDirectory(conf, zos, baseName, root, status.getPath());
			}
			LOG.info(String.format("Wrote %d entries for directory %s", count, itemToZip));
			return count;
		}

		final String inZipPath = relativePathForZipEntry(itemToZip.toUri().getPath(), baseName, root);

		if(inZipPath.length() == 0) {
			LOG.warn(String.format("Skipping empty zip file path for %s (%s %s)", itemToZip, root, baseName));
			return 0;
		}

		// Take empty files in case the place holder is needed
		FSDataInputStream in = null;
		try {
			in = localFs.open(itemToZip);
			final ZipEntry ze = new ZipEntry(inZipPath);
			ze.setTime(itemStatus.getModificationTime());
			// Comments confuse looking at the zip file
			// ze.setComment(itemToZip.toString());
			zos.putNextEntry(ze);

			IOUtils.copyBytes(in, zos, conf, false);
			zos.closeEntry();
			LOG.info(String.format("Wrote %d entries for file %s", count, itemToZip));
			return 1;
		} finally {
			in.close();
		}

	}

	static String relativePathForZipEntry(final String rawPath, final String baseName, final String root) {
		String relativePath = rawPath.replaceFirst(Pattern.quote(root.toString()), "");
		LOG.info(String.format("RawPath %s, baseName %s, root %s, first %s", rawPath, baseName, root, relativePath));

		if(relativePath.startsWith(Path.SEPARATOR)) {
			relativePath = relativePath.substring(1);
		}
		LOG.info(String.format("RawPath %s, baseName %s, root %s, post leading slash %s", rawPath, baseName, root,
		    relativePath));
		if(relativePath.isEmpty()) {
			LOG.warn(String.format("No data after root (%s) removal from raw path %s", root, rawPath));
			return baseName;
		}
		// Construct the path that will be written to the zip file, including
		// removing any leading '/' characters
		String inZipPath = baseName + Path.SEPARATOR_CHAR + relativePath;

		LOG.info(String.format("RawPath %s, baseName %s, root %s, inZip 1 %s", rawPath, baseName, root, inZipPath));
		if(inZipPath.startsWith(Path.SEPARATOR)) {
			inZipPath = inZipPath.substring(1);
		}
		LOG.info(String.format("RawPath %s, baseName %s, root %s, inZip 2 %s", rawPath, baseName, root, inZipPath));

		return inZipPath;

	}

	static boolean setLogLevel(String packageName, String level) {
		Log logger = LogFactory.getLog(packageName);
		if(logger == null) {
			return false;
		}
		// look for: org.apache.commons.logging.impl.SLF4JLocationAwareLog
		LOG.warn("logger class:" + logger.getClass().getName());
		if(logger instanceof Log4JLogger) {
			process(((Log4JLogger) logger).getLogger(), level);
			return true;
		}
		if(logger instanceof Jdk14Logger) {
			process(((Jdk14Logger) logger).getLogger(), level);
			return true;
		}
		return false;
	}

	public static void process(org.apache.log4j.Logger log, String level) {
		if(level != null) {
			log.setLevel(org.apache.log4j.Level.toLevel(level));
		}
	}

	public static void process(java.util.logging.Logger log, String level) {
		if(level != null) {
			log.setLevel(java.util.logging.Level.parse(level));
		}
	}
}
