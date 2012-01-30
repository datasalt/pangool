package com.datasalt.avrool; 







/** The wrapper of data for jobs configured with {@link AvroJob} . */
public class PangoolWrapper<T> {
  private T datum;

  /** Wrap null. Construct {@link AvroWrapper} wrapping no datum. */
  public PangoolWrapper() { this(null); }

  /** Wrap a datum. */
  public PangoolWrapper(T datum) { this.datum = datum; }

  /** Return the wrapped datum. */
  public T datum() { return datum; }

  /** Set the wrapped datum. */
  public void datum(T datum) { this.datum = datum; }
  
  public int hashCode() {
    return (datum == null) ? 0 : datum.hashCode();
  }

  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    PangoolWrapper that = (PangoolWrapper)obj;
    if (this.datum == null) {
      if (that.datum != null)
        return false;
    } else if (!datum.equals(that.datum))
      return false;
    return true;
  }
    
}
