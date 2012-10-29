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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.pangool.io.ITuple;

/**
 * Instantiable OutputFormat that can be used in Pangool for indexing {@link ITuple} in SOLR. It behaves similar to
 * SOLR-1301's SolrOutputFormat with the difference that configuration is passed via instance (constructor params). This
 * allows us to easily have multiple {@link TupleSolrOutputFormat} in the same Pangool Job. Also, it is much easier to
 * configure: it just needs to be instantiated (no need to call multiple static methods to configure it). Everything
 * will be configured underneath.
 * <p>
 * Things that can be configured via constructor:
 * <ul>
 * <li>solrHome: Local folder where schema.xml and such can be found.</li>
 * <li>converter: An implementation of {@link TupleDocumentConverter} other than {@link DefaultTupleDocumentConverter}
 * if user wants to do complex things like boosting.</li>
 * <li>outputZipFile: If user wants to produce a ZIP with the index.</li>
 * <li>batchSize: Number of documents that will go in each indexing batch.</li>
 * <li>threadCount: Number of threads in a pool that will be used for indexing.</li>
 * <li>queueSize: Maximum number of batches that can be pooled in the batch indexing thread pool.</li>
 * </ul>
 * For a usage example see test class {@link TupleSolrOutputFormatExample}.
 */
@SuppressWarnings("serial")
public class TupleSolrOutputFormat extends FileOutputFormat<ITuple, NullWritable> implements Serializable {

	private static final Log LOG = LogFactory.getLog(TupleSolrOutputFormat.class);

	/**
	 * The base name of the zip file containing the configuration information. This file is passed via the distributed
	 * cache using a unique name, obtained via {@link #getZipName(Configuration jobConf)}.
	 */
	public static final String ZIP_FILE_BASE_NAME = "solr.zip";

	static int defaultSolrWriterThreadCount = 2;
	static int defaultSolrWriterQueueSize = 100;
	static int defaultSolrBatchSize = 20;

	/**
	 * Size of the batch size used for indexing
	 */
	private int batchSize = defaultSolrBatchSize;
	/**
	 * The number of threads that will be used for indexing
	 */
	private int threadCount = defaultSolrWriterThreadCount;
	/**
	 * The SOLR writer queue size
	 */
	private int queueSize = defaultSolrWriterQueueSize;

	/**
	 * Whether the output should be a ZIP of the index or not
	 */
	private boolean outputZipFile = false;

	/**
	 * The zip file name used for sending the SOLR configuration in the DistributedCache
	 */
	private String zipName;
	/**
	 * The local SOLR home to be used if this is run in Hadoop's local mode (no DC)
	 */
	private String localSolrHome;
	/**
	 * The document converter. By default, {@link DefaultTupleDocumentConverter}, but users may implement custom ones for
	 * boosting, etc.
	 */
	private TupleDocumentConverter converter;

	@Override
	public void checkOutputSpecs(JobContext job) throws IOException {
		super.checkOutputSpecs(job);
	}

	@Override
	public RecordWriter<ITuple, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException,
	    InterruptedException {
		return new SolrRecordWriter(batchSize, outputZipFile, threadCount, queueSize, localSolrHome, zipName, converter,
		    context);
	}

	public TupleSolrOutputFormat(File solrHome, Configuration hadoopConf) throws IOException {
		setupSolrHomeCache(solrHome, hadoopConf);
		this.converter = new DefaultTupleDocumentConverter();
	}

	public TupleSolrOutputFormat(File solrHome, Configuration hadoopConf, TupleDocumentConverter converter)
	    throws IOException {
		this(solrHome, hadoopConf);
		this.converter = converter;
	}

	public TupleSolrOutputFormat(File solrHome, Configuration hadoopConf, TupleDocumentConverter converter,
	    boolean outputZipFile, int batchSize, int threadCount, int queueSize) throws IOException {
		this(solrHome, hadoopConf, converter);
		this.outputZipFile = outputZipFile;
		this.batchSize = batchSize;
		this.threadCount = threadCount;
		this.queueSize = queueSize;
	}

	private void setupSolrHomeCache(File solrHome, Configuration conf) throws IOException {
		if(solrHome == null || !(solrHome.exists() && solrHome.isDirectory())) {
			throw new IOException("Invalid solr.home: " + solrHome);
		}
		localSolrHome = solrHome.getAbsolutePath();
		File tmpZip = File.createTempFile("solr", "zip");
		createZip(solrHome, tmpZip);
		// Make a reasonably unique name for the zip file in the distributed cache
		// to avoid collisions if multiple jobs are running.
		String hdfsZipName = UUID.randomUUID().toString() + '.' + ZIP_FILE_BASE_NAME;
		zipName = hdfsZipName;

		Path zipPath = new Path("/tmp", zipName);
		FileSystem fs = FileSystem.get(conf);
		fs.copyFromLocalFile(new Path(tmpZip.toString()), zipPath);
		final URI baseZipUrl = fs.getUri().resolve(zipPath.toString() + '#' + zipName);

		DistributedCache.addCacheArchive(baseZipUrl, conf);
		LOG.debug("Set Solr cache: " + Arrays.asList(DistributedCache.getCacheArchives(conf)));
	}

	private static void createZip(File dir, File out) throws IOException {
		HashSet<File> files = new HashSet<File>();
		// take only conf/ and lib/
		for(String allowedDirectory : SolrRecordWriter.getAllowedConfigDirectories()) {
			File configDir = new File(dir, allowedDirectory);
			boolean configDirExists;
			/** If the directory does not exist, and is required, bail out */
			if(!(configDirExists = configDir.exists()) && SolrRecordWriter.isRequiredConfigDirectory(allowedDirectory)) {
				throw new IOException(String.format("required configuration directory %s is not present in %s",
				    allowedDirectory, dir));
			}
			if(!configDirExists) {
				continue;
			}
			listFiles(configDir, files); // Store the files in the existing, allowed
			                             // directory configDir, in the list of files
			                             // to store in the zip file
		}

		out.delete();
		int subst = dir.toString().length();
		ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(out));
		byte[] buf = new byte[1024];
		for(File f : files) {
			ZipEntry ze = new ZipEntry(f.toString().substring(subst));
			zos.putNextEntry(ze);
			InputStream is = new FileInputStream(f);
			int cnt;
			while((cnt = is.read(buf)) >= 0) {
				zos.write(buf, 0, cnt);
			}
			is.close();
			zos.flush();
			zos.closeEntry();
		}
		zos.close();
	}

	private static void listFiles(File dir, Set<File> files) throws IOException {
		File[] list = dir.listFiles();
		for(File f : list) {
			if(f.isFile()) {
				files.add(f);
			} else {
				listFiles(f, files);
			}
		}
	}
}
