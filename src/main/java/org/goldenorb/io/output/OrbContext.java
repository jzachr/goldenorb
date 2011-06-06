package org.goldenorb.io.output;

public class OrbContext<KEY, VALUE> {
	private KEY key;
	private VALUE value;
	
	public void write(KEY key, VALUE value){
		this.key = key;
		this.value = value;
	}
	
	public KEY getKey(){
		return key;
	}
	
	public VALUE getValue(){
		return value;
	}
}
