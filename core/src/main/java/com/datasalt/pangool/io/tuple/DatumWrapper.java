package com.datasalt.pangool.io.tuple;

public class DatumWrapper<T> {

	private T currentDatum;
	private T previousDatum;
	
	public DatumWrapper(T datum){
		this.currentDatum = datum;
	}
	
	public DatumWrapper(){
		
	}
	
	
	public T currentDatum(){
		return currentDatum;
	}
	
	public void datum(T datum){
		this.currentDatum = datum;
	}
	
	public T previousDatum(){
		return previousDatum;
	}
	
	public void swapInstances(){
		T tmp = currentDatum;
		this.currentDatum = previousDatum;
		this.previousDatum = tmp;
	}
	
	@Override
	public String toString(){
		if (currentDatum != null){
			return currentDatum.toString();
		} else {
			return "";
		}
	}
	
	
}
