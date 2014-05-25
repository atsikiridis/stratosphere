/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.hadoopcompatibility.mapred;

import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.operators.translation.TupleUnwrappingIterator;
import eu.stratosphere.api.java.record.operators.ReduceOperator.Combinable;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.hadoopcompatibility.mapred.utils.HadoopConfiguration;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopDummyReporter;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopOutputCollector;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.InstantiationUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;

/**
 * The wrapper for a Hadoop Reducer (mapred API).
 */
/*TODO Modify class signature as soon as the TypeExtractor supports non identical generic input/output types and type
erasure issues are fixed.*/

@Combinable //TODO Probably not all the times.
public class HadoopReduceFunction<IN extends Tuple2<? extends WritableComparable, ? extends Writable>, OUT extends Tuple2<? extends WritableComparable, ? extends Writable>>
		extends GroupReduceFunction<IN, OUT>
		implements Serializable {

	private static final long serialVersionUID = 1L;

	private JobConf jobConf;
	private Reducer reducer;
	private String reducerName;
	private HadoopOutputCollector<OUT> outputCollector;
	private Reporter reporter;
	private ReducerTransformingIterator iterator;

	@SuppressWarnings("unchecked")
	public HadoopReduceFunction(JobConf jobConf) {
		this(jobConf, new HadoopOutputCollector<OUT>(), new HadoopDummyReporter());
	}

	public HadoopReduceFunction(JobConf jobConf, HadoopOutputCollector<OUT> outputCollector, Reporter reporter) {
		this.jobConf = jobConf;
		this.reducer = InstantiationUtil.instantiate(jobConf.getReducerClass());
		this.reducerName = reducer.getClass().getName();
		this.outputCollector = outputCollector;
		this.iterator = new ReducerTransformingIterator();
		this.reporter = reporter;
	}

	/**
	 * A wrapping iterator for an iterator of key-value tuples that can be used as an iterator of values. Moreover,
	 * there is always a reference to the key corresponding to the value that is currently being traversed.
	 */
	public static class ReducerTransformingIterator<T,K> extends TupleUnwrappingIterator<T,K> implements java.io.Serializable {

		private static final long serialVersionUID = 1L;
		private Iterator<Tuple2<K,T>> iterator;
		private K key;
		private Tuple2<K,T> first;

		@Override()
		public void set(Iterator<Tuple2<K,T>> iterator) {
			this.iterator = iterator;

			if(this.hasNext()) {
				this.first = iterator.next();
				this.key = this.first.f0;
			}
		}

		@Override
		public boolean hasNext() {
			if(this.first != null) {
				return true;
			}
			return iterator.hasNext();
		}

		@Override
		public T next() {
			if(this.first != null) {
				T val = this.first.f1;
				this.first = null;
				return val;
			}
			final Tuple2<K,T> tuple = iterator.next();
			this.key = tuple.f0;
			return tuple.f1;
		}

		public Object getKey() {
			return this.key;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void reduce(Iterator<IN> values, Collector<OUT> out) throws Exception {
		outputCollector.set(out);
		iterator.set(values);
		this.reducer.reduce(iterator.getKey() , iterator, outputCollector, reporter);
	}

	/**
	 * Custom serialization methods.
	 *  @see http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeUTF(reducerName);
		jobConf.write(out);
		out.writeObject(iterator);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		reducerName = in.readUTF();
		if(jobConf == null) {
			jobConf = new JobConf();
		}
		jobConf.readFields(in);
		try {
			this.reducer = (Reducer) Class.forName(this.reducerName).newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop reducer", e);
		}
		ReflectionUtils.setConf(reducer, jobConf);
		outputCollector = (HadoopOutputCollector) InstantiationUtil.instantiate(
				HadoopConfiguration.getOutputCollectorFromConf(jobConf));
		reporter = InstantiationUtil.instantiate(
				HadoopConfiguration.getReporterFromConf(jobConf));
		iterator = (ReducerTransformingIterator) in.readObject();
	}



}
