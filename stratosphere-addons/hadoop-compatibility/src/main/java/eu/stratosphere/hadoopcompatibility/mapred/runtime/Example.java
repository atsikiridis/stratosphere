package eu.stratosphere.hadoopcompatibility.mapred.runtime;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.hadoopcompatibility.mapred.StratosphereHadoopJobClient;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.util.Collector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by artem on 6/15/14.
 */
public class Example {
	public static void main(String[] args) throws Exception {
		HadoopLocalEnvironment env = (HadoopLocalEnvironment) HadoopEnvironment.getExecutionEnvironment();

		List<Tuple2<String, Long>> data = new ArrayList<Tuple2<String, Long>>();
		data.add(new Tuple2<String, Long>(new String("allen"), new Long(1)));
		data.add(new Tuple2<String, Long>(new String("allein"), new Long(1)));
		data.add(new Tuple2<String, Long>(new String("allein"), new Long(1)));
		data.add(new Tuple2<String, Long>(new String("a"), new Long(1)));
		data.add(new Tuple2<String, Long>(new String("word"), new Long(1)));
		data.add(new Tuple2<String, Long>(new String("word"), new Long(1)));
		data.add(new Tuple2<String, Long>(new String("words"), new Long(1)));

		DataSet<Tuple2<String, Long>> myData = env.fromCollection(data);

		DataSet<Tuple2<Text, LongWritable>> res = myData
				.map(new MapFunction<Tuple2<String, Long>, Tuple2<Text, LongWritable>>() {

					@Override
					public Tuple2<Text, LongWritable> map(
							Tuple2<String, Long> value) throws Exception {
						return new Tuple2<Text, LongWritable>(new Text(value.f0), new LongWritable(value.f1));
					}
				})
				.groupBy(0)
				.reduceGroup(
						new GroupReduceFunction<Tuple2<Text,LongWritable>, Tuple2<Text,LongWritable>>() {

							@Override
							public void reduce(
									Iterator<Tuple2<Text, LongWritable>> values, Collector<Tuple2<Text, LongWritable>> out) {

								long sum = 0;
								Tuple2<Text,LongWritable> v = null;
								while(values.hasNext()) {
									v = values.next();
									sum += v.f1.get();
								}
								v.f1 = new LongWritable(sum);
								out.collect(v);
							}
						}
				);
		res.print();
		//env.execute();
		HadoopLocalExecutor executor = ((HadoopLocalEnvironment) env).getExecutor("TEST");
		HadoopInternalJobClient jobClient = (HadoopInternalJobClient) executor.getJobClient();
		JobSubmissionResult result = jobClient.submitJob();  //SEND IT TO WAITFORCOMPLETION boommmmmm
		jobClient.waitForCompletion(result);
		executor.stop();
	}
}
