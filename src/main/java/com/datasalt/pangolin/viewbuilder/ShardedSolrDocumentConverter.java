package com.datasalt.pangolin.viewbuilder;

import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.SolrDocumentConverter;


/**
 * 
 * The aim of this {@link SolrDocumentConverter} is to generate document ids used as unique keys <uniqueKey></uniqueKey> that are unique in the entire 
 * collection ,i.e there are no repeated ids in the shards that contain the same collection(core). This is a restriction of Solr'<a href="http://wiki.apache.org/solr/DistributedSearch">DistributedSearch</a>
 * 
 * 
 * @author epalace
 *
 * @param <KEY_TYPE>
 * @param <VALUE_TYPE>
 */
public abstract class ShardedSolrDocumentConverter<KEY_TYPE,VALUE_TYPE> extends SolrDocumentConverter<KEY_TYPE,VALUE_TYPE> {

	
	public static final String PARTITIONER_CONF="sharded_solr.partitioner";
	
	private static final Logger log = Logger.getLogger(ShardedSolrDocumentConverter.class);
	
  private Integer currentShard;
  private int numShards;
  private int shardNumberShift;
  private Long lastRecordId=null;
  private Partitioner<KEY_TYPE,VALUE_TYPE> partitioner;
	
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setConf(Configuration conf) {
		super.setConf(conf);
		if(conf != null) {
			JobConf jobConf = (JobConf) conf;
			numShards = jobConf.getNumReduceTasks();
			shardNumberShift = (int)Math.ceil(numDigits(Long.MAX_VALUE)) - 1 - (int) Math.ceil(numDigits(numShards));
			log.info("Num shards : " + numShards);
			
      Class partitionerClass;
      try {
	      partitionerClass = Class.forName(jobConf.get(PARTITIONER_CONF));
      } catch(ClassNotFoundException e) {
	      e.printStackTrace();
	      throw new RuntimeException("Partitioner not set.Use conf.set(ShardedSolrDocument.PARTITIONER_CONF,partitionerClassName)",e);
      }
      log.info("Partitioner class : " + partitionerClass);
			partitioner = (Partitioner<KEY_TYPE,VALUE_TYPE>) ReflectionUtils.newInstance(partitionerClass, conf);
		}
	}
	
	@Override
	public Collection<SolrInputDocument> convert(KEY_TYPE key, VALUE_TYPE value) {
		if (currentShard == null){
			// a partitioner is used to calculate which reducer task is running.
			// this is done just once, because all the records received in this reducer have the same reducer number.
			currentShard = partitioner.getPartition(key,value,numShards);
			lastRecordId =  (((long)currentShard * (long)Math.pow(10l,shardNumberShift)));
			log.info("Current shard:" + currentShard + " First id : " + lastRecordId);
		}
		return null;
	}
	
	public long generateUniqueKeyId(){
		return ++lastRecordId;	//TODO check if record Id is too high ??
	}
	
	public static double numBits(long num){
		return Math.log(num)/Math.log(2);
	}
	
	public static double numDigits(long num){
		return Math.log10(num);
	}
	
}