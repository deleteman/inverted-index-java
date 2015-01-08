package com.globant.training.invertedIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class WordIntArrayDict implements Writable {
	
	private HashMap<Text, HashSet<IntWritable>> data;
	
	private HashMap<Text, HashSet<IntWritable>> __emptyInit() {
		return new HashMap<Text, HashSet<IntWritable>>();		
	}
	public WordIntArrayDict() {
		data = __emptyInit();	
	}
	public WordIntArrayDict(Text k, IntWritable v ) {
		data = __emptyInit();
		HashSet<IntWritable> dummyList = new HashSet<IntWritable>();
		dummyList.add(v);
		data.put(k, dummyList);	
	}
	public WordIntArrayDict(Text k, HashSet<IntWritable> vs ) {
		data = __emptyInit();
		data.put(k, vs);
	}
	
	public void addValue(Text k, IntWritable i) {
		HashSet<IntWritable> dummyList = data.get(k);
		Boolean nullList = false;

		if(dummyList == null) {
			dummyList = new HashSet<IntWritable>();
			nullList = true;
		}
		dummyList.add(i);
		if(nullList) {
			data.put(k,  dummyList);
		}
	}
	public void addValues(Text k, HashSet<IntWritable> arr) {
		HashSet<IntWritable> dummyList = data.get(k);
		Boolean nullList = false;
		if(dummyList == null) {
			dummyList = new HashSet<IntWritable>();
			nullList = true;
		}
		
		dummyList.addAll(arr);
		if(nullList) {
			data.put(k, dummyList);
		}
	}
	
	public Set<Text> getKeys() {
		return data.keySet();
	}
	
	public HashSet<IntWritable> get(Text key) {
		return data.get(key);
	}
	
	public int size() {
		return data.keySet().size();
	}

	public void readFields(DataInput in) throws IOException {
		int numKeys = in.readInt();
		int numValues = 0;
		data.clear();
		for(int i = 0; i < numKeys; i++) {
			Text key = new Text();
			key.readFields(in);
			numValues = in.readInt();
			HashSet<IntWritable> tmpList = new HashSet<IntWritable>();
			for(int vindex = 0; vindex < numValues; vindex++) {
				IntWritable iw = new IntWritable();
				iw.readFields(in);
				tmpList.add(iw);
			}
			data.put(key, tmpList);
		}
		
	}
	/**
	 * Serialize the content of the hashmap like so:
	 * [NUM_KEYS](key[NUM_VALUES]values)
	 */
	public void write(DataOutput out) throws IOException {
		out.writeInt(data.keySet().size());
		for(Map.Entry<Text, HashSet<IntWritable>> elem : data.entrySet()) {
			elem.getKey().write(out);
			out.writeInt(elem.getValue().size());
			for(IntWritable i : elem.getValue()) {
				i.write(out);
			}
		}		
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		return result;
	}

	public String toString() {
		StringBuilder tmp = new StringBuilder();
		StringBuilder indexList = new StringBuilder();
		String bigListDelimiter = "";
		for(Map.Entry<Text, HashSet<IntWritable>> elem : data.entrySet()) {
			String filename = elem.getKey().toString();
			String listDelimiter = "";
			tmp.append(bigListDelimiter).append("(");
			tmp.append(filename);
			tmp.append(",");
		
			for(IntWritable i : elem.getValue()) {
				indexList
					.append(listDelimiter)
					.append(i.toString());
				listDelimiter = ",";
			}
			tmp.append(indexList.toString());
			tmp.append(")");
			indexList = new StringBuilder();
			bigListDelimiter = ",";
		}	
		String output = tmp.toString();		
		return  output;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WordIntArrayDict other = (WordIntArrayDict) obj;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		
		return true;
	}
	

}
