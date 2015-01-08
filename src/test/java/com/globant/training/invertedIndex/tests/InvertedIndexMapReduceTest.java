package com.globant.training.invertedIndex.tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.globant.training.invertedIndex.InvertedIndexMapper;
import com.globant.training.invertedIndex.InvertedIndexReducer;
import com.globant.training.invertedIndex.WordIntArrayDict;

public class InvertedIndexMapReduceTest {

	MapDriver<LongWritable, Text, Text, WordIntArrayDict> mapDriver;
	ReduceDriver<Text, WordIntArrayDict, Text, WordIntArrayDict> reduceDriver;
	final Text testFilename = new Text("somefile");

	
	@Before
	public void setUp() {
		InvertedIndexMapper mapper = new InvertedIndexMapper();
		InvertedIndexReducer reducer = new InvertedIndexReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}
	
	@Test
	public void testMapper() throws IOException{
		mapDriver.withInput(new LongWritable(0), new Text("This is a test"));
		WordIntArrayDict output1 = new WordIntArrayDict();
		output1.addValue(testFilename, new IntWritable(1));
		
		WordIntArrayDict output2 = new WordIntArrayDict();
		output2.addValue(testFilename, new IntWritable(6));
		
		WordIntArrayDict output3 = new WordIntArrayDict();
		output3.addValue(testFilename, new IntWritable(9));
		
		WordIntArrayDict output4 = new WordIntArrayDict();
		output4.addValue(testFilename, new IntWritable(11));
		
		
		mapDriver.withOutput(new Text("This"), output1);
		mapDriver.withOutput(new Text("is"), output2);
		mapDriver.withOutput(new Text("a"), output3);
		mapDriver.withOutput(new Text("test"), output4);
		mapDriver.runTest(true);
	}
	
	@Test
	public void testReducer() throws IOException {
		WordIntArrayDict input = new WordIntArrayDict();
		input.addValue(testFilename, new IntWritable(1));
		input.addValue(new Text("somefile2"), new IntWritable(1));
		input.addValue(testFilename, new IntWritable(4));
		input.addValue(testFilename, new IntWritable(5));
	
		ArrayList<WordIntArrayDict> inputList = new ArrayList<WordIntArrayDict>();
		inputList.add(input);
		
		reduceDriver.withInput(new Text("word"), inputList);
		
		WordIntArrayDict output = new WordIntArrayDict();
		HashSet<IntWritable> idx1 = new HashSet<IntWritable>();
		idx1.add(new IntWritable(1));
		idx1.add(new IntWritable(4));
		idx1.add(new IntWritable(5));
		output.addValues(testFilename, idx1);
		output.addValue(new Text("somefile2"), new IntWritable(1));
		
		reduceDriver.withOutput(new Text("word"), output);
		reduceDriver.runTest(true);
	}
}
