package com.globant.training.invertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndexDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new  Configuration(), new InvertedIndexDriver(), args);
		System.exit(res);	
	}
	
	public int run(String[] args) throws Exception {
		 if (args.length != 2) {
            System.out.println("Usage: hadoop jar jarFile mainClass"
                                       + " [generic options] <in> <out>");
            return 1;
        }
        Job job = new Job( getConf(), "InvertedIndex" );
        job.setJarByClass( getClass() );
        job.setMapperClass( InvertedIndexMapper.class );
        job.setCombinerClass( InvertedIndexReducer.class );
        job.setReducerClass( InvertedIndexReducer.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( WordIntArrayDict.class );
        
        FileInputFormat.addInputPath( job, new Path(args[0]) );
        FileOutputFormat.setOutputPath( job, new Path(args[1]) );
        boolean success = job.waitForCompletion( true );
        return success ? 0 : 1;

	}

}
