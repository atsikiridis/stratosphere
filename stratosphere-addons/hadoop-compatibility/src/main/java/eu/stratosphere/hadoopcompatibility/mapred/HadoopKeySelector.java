package eu.stratosphere.hadoopcompatibility.mapred;

import eu.stratosphere.api.common.typeutils.base.IntComparator;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.hadoopcompatibility.mapred.utils.HadoopConfiguration;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopReporter;
import eu.stratosphere.util.InstantiationUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class HadoopKeySelector<K2,V2> extends KeySelector<Tuple2<K2,V2>, Integer> {

	private Partitioner partitioner;
	private int noOfReduceTasks;
	private JobConf jobConf;

	public HadoopKeySelector(Partitioner partitioner, int noOfReduceTasks) {
		this.partitioner = partitioner;
		this.noOfReduceTasks = noOfReduceTasks;
		this.jobConf = new JobConf();
	}

	@Override
	public Integer getKey(final Tuple2<K2, V2> value) {
		return this.partitioner.getPartition(value.f0,value.f1, this.noOfReduceTasks);
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		jobConf.setPartitionerClass(partitioner.getClass());
		jobConf.write(out);
		out.writeObject(this.noOfReduceTasks);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		jobConf = new JobConf();
		jobConf.readFields(in);
		try {
			this.partitioner = InstantiationUtil.instantiate(this.jobConf.getPartitionerClass());
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop mapper", e);
		}
		ReflectionUtils.setConf(partitioner, jobConf);
		this.noOfReduceTasks = (Integer) in.readObject();
	}


}
