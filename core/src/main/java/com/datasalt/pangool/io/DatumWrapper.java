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
package com.datasalt.pangool.io;

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
