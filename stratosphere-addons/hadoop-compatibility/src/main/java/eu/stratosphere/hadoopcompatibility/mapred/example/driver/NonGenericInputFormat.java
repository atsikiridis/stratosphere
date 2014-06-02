package eu.stratosphere.hadoopcompatibility.mapred.example.driver;

import eu.stratosphere.api.java.io.TextInputFormat;
import eu.stratosphere.hadoopcompatibility.mapred.StratosphereHadoopJobClient;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.TokenCountMapper;

import java.io.IOException;

/**
 * A regular Hadoop WordCount driver that runs on Stratosphere (see last line).
 */

public class NonGenericInputFormat {

	public static void main(String[] args) throws Exception{
		final String inputPath = args[0];
		final String outputPath = args[1];

		final JobConf conf = new JobConf();

		conf.setInputFormat(CustomTextInputFormat.class);
		org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

		conf.setOutputFormat(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapperClass(TestTokenizeMap.class);
		conf.setReducerClass(LongSumReducer.class);
		conf.setCombinerClass((LongSumReducer.class));

		conf.set("mapred.textoutputformat.separator", " ");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		//The only line of Stratoshere code!
		StratosphereHadoopJobClient.runJob(conf).waitForCompletion();
	}


	public static class TestTokenizeMap<K> extends TokenCountMapper<K> {
		@Override
		public void map(K key, Text value, OutputCollector<Text, LongWritable> output,
						Reporter reporter) throws IOException {
			final Text strippedValue = new Text(value.toString().toLowerCase().replaceAll("\\W+", " "));
			super.map(key, strippedValue, output, reporter);
		}
	}

	public static class CustomTextInputFormat extends org.apache.hadoop.mapred.TextInputFormat {
	}
}
