package com.datasalt.pangool;
public class PangoolRuntimeException extends RuntimeException {
  public PangoolRuntimeException(Throwable cause) { super(cause); }
  public PangoolRuntimeException(String message) { super(message); }
  public PangoolRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }
}