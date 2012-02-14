package com.datasalt.pangool.io.tuple;

public class DatumWrapper<T> {

	private T currentDatum;
	private T previousDatum;
	
	public DatumWrapper(T datum){
		this.currentDatum = datum;
	}
	
	public DatumWrapper(T current,T previous){
		this.currentDatum = current;
		this.previousDatum = previous;
	}
	
	public DatumWrapper(){
		
	}
	
	public T datum(){
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
		StringBuilder b = new StringBuilder();
		b.append("{ current:").append(currentDatum);
		b.append("\n  previous:").append(previousDatum);
		b.append("}");
		return b.toString();
	}
}
