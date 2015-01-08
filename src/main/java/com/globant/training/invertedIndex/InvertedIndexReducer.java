package com.globant.training.invertedIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends
		Reducer<Text, WordIntArrayDict, Text, WordIntArrayDict> {

	/**
	 * input:
	 * word, [Object(filename, index), Object(filename, index)]
	 * 
	 * output:
	 * 
	 * word, [(filename, [index,]),]
	 * i.e: 
	 */
	@Override
	protected void reduce(Text key, Iterable<WordIntArrayDict> values,
			Reducer<Text, WordIntArrayDict, Text, WordIntArrayDict>.Context context)
			throws IOException, InterruptedException {
		
		WordIntArrayDict data = new WordIntArrayDict();	
	
	
		for(WordIntArrayDict dict : values) {
			Set <Text> filenames = dict.getKeys();
			for(Text filename : filenames) {
				data.addValues(filename, dict.get(filename));			
			}
			
		}
		context.write(key, data);
	}

	

}
