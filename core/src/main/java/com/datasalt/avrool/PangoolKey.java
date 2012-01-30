package com.datasalt.avrool; 

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.record.Record;


/** The wrapper of data for jobs configured with {@link AvroJob} . */
public class PangoolKey<T> /* implements Configurable */{
  private T currentDatum;
  private T previousDatum;
  private Configuration conf;

  /** Wrap null. Construct {@link AvroWrapper} wrapping no currentDatum. */
  public PangoolKey() { this(null); }

  /** Wrap a currentDatum. */
  public PangoolKey(T datum) { this.currentDatum = datum; }

  /** Return the wrapped currentDatum. */
  public T datum() { return currentDatum; }

  public T previousDatum() { return previousDatum; }
  
  /** Set the wrapped currentDatum. */
  public void datum(T datum) { this.currentDatum = datum; }
  
  public int hashCode() {
    return (currentDatum == null) ? 0 : currentDatum.hashCode();
  }

  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    PangoolKey that = (PangoolKey)obj;
    if (this.currentDatum == null) {
      if (that.currentDatum != null)
        return false;
    } else if (!currentDatum.equals(that.currentDatum))
      return false;
    return true;
  }
  
  public void swapInstances(){
  	T temp = this.currentDatum;
  	this.currentDatum = this.previousDatum;
  	this.previousDatum = temp;
  }
    
}
