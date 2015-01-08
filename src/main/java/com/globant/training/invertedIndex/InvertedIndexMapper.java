package com.globant.training.invertedIndex;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends
		Mapper<LongWritable, Text, Text, WordIntArrayDict> {

	/**
	 * output:
	 * word, Object(filename, index)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString().replaceAll("[,.|\"'\t]", "");
		
		String word = "";
		String[] words = line.split( " " );
		FileSplit currentFile = (FileSplit)context.getInputSplit();
		String filename = currentFile.getPath().getName();
		int index = 0;
		int currentOffset = Integer.parseInt(key.toString());
		
		for(int i = 0; i < words.length; i++) {
			word = words[i];
			index = currentOffset + getOffsetUptoWord(words, i, context);
			WordIntArrayDict output = new WordIntArrayDict(
				  	new Text(filename), 
				  	new IntWritable(index));
			context.write(new Text(word), output );
		}

	}

	private int getOffsetUptoWord(
			String[] words,
			int i,
			Mapper<LongWritable, Text, Text, WordIntArrayDict>.Context context) {
		int length = 0;
		String[] wordsUpTo = Arrays.copyOfRange(words, 0, i);
		for(String w : wordsUpTo) {
			length += w.length();
		}
		Configuration conf = context.getConfiguration();
		int offset = conf.getInt("mapreduce.map.input.start",0);
		return (length + i + 1) + offset;
				
	}
}
