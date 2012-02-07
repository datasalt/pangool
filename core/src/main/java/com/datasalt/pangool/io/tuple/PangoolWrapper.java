package com.datasalt.pangool.io.tuple;

public class PangoolWrapper<T> {

	private T currentInstance;
	private T previousInstance;
	
	public T currentDatum(){
		return currentInstance;
	}
	
	public void currentDatum(T datum){
		this.currentInstance = datum;
	}
	
	public T previousDatum(){
		return previousInstance;
	}
	
	public void swapInstances(){
		T tmp = currentInstance;
		this.currentInstance = previousInstance;
		this.previousInstance = tmp;
	}
	
}
