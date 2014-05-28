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
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.ResultTypeQueryable;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeInfoParser;
import eu.stratosphere.api.java.typeutils.WritableTypeInfo;
import eu.stratosphere.hadoopcompatibility.mapred.utils.HadoopConfiguration;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopDummyReporter;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopOutputCollector;
import eu.stratosphere.types.TypeInformation;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.InstantiationUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
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
public class HadoopReduceFunction<KEYIN extends WritableComparable, VALUEIN extends Writable,
		KEYOUT extends WritableComparable, VALUEOUT extends Writable> extends GroupReduceFunction<Tuple2<KEYIN,VALUEIN>,
		Tuple2<KEYOUT,VALUEOUT>> implements Serializable, ResultTypeQueryable<Tuple2<KEYOUT,VALUEOUT>> {

	private static final long serialVersionUID = 1L;

	private final Class<KEYOUT> keyoutClass;
	private final Class<VALUEOUT> valueoutClass;

	private JobConf jobConf;
	private Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> reducer;
	private HadoopOutputCollector<KEYOUT,VALUEOUT> outputCollector;
	private Reporter reporter;
	private ReducerTransformingIterator iterator;

	public HadoopReduceFunction(JobConf jobConf) {
		this(jobConf, new HadoopOutputCollector<KEYOUT,VALUEOUT>(), new HadoopDummyReporter());
	}

	@SuppressWarnings("unchecked")
	public HadoopReduceFunction(JobConf jobConf, HadoopOutputCollector<KEYOUT,VALUEOUT> outputCollector,
								Reporter reporter) {
		this.jobConf = jobConf;
		this.reducer = InstantiationUtil.instantiate(jobConf.getReducerClass());
		this.outputCollector = outputCollector;
		this.iterator = new ReducerTransformingIterator();
		this.reporter = reporter;
		this.keyoutClass = (Class<KEYOUT>) this.jobConf.getOutputKeyClass();
		this.valueoutClass = (Class<VALUEOUT>) this.jobConf.getOutputValueClass();
	}

	/**
	 * A wrapping iterator for an iterator of key-value tuples that can be used as an iterator of values. Moreover,
	 * there is always a reference to the key corresponding to the value that is currently being traversed.
	 */
	private final class ReducerTransformingIterator extends TupleUnwrappingIterator<VALUEIN,KEYIN>
			implements java.io.Serializable {//ResultTypeQueryable<Tuple2<KEYOUT,VALUEOUT>> {

		private static final long serialVersionUID = 1L;
		private Iterator<Tuple2<KEYIN,VALUEIN>> iterator;
		private KEYIN key;
		private Tuple2<KEYIN,VALUEIN> first;

		@Override()
		public void set(Iterator<Tuple2<KEYIN,VALUEIN>> iterator) {
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
		public VALUEIN next() {
			if(this.first != null) {
				final VALUEIN val = this.first.f1;
				this.first = null;
				return val;
			}
			final Tuple2<KEYIN,VALUEIN> tuple = iterator.next();
			return tuple.f1;
		}

		private KEYIN getKey() {
			return WritableUtils.clone(this.key, jobConf);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public void reduce(Iterator<Tuple2<KEYIN,VALUEIN>> values, Collector<Tuple2<KEYOUT,VALUEOUT>> out) throws Exception {
		outputCollector.set(out);
		iterator.set(values);
		this.reducer.reduce(iterator.getKey(), iterator, outputCollector, reporter);
	}

	@Override
	public TypeInformation<Tuple2<KEYOUT,VALUEOUT>> getProducedType() {
		final WritableTypeInfo<KEYOUT> keyTypeInfo = new WritableTypeInfo<KEYOUT>(keyoutClass);
		final WritableTypeInfo<VALUEOUT> valueTypleInfo = new WritableTypeInfo<VALUEOUT>(valueoutClass);
		return new TupleTypeInfo<Tuple2<KEYOUT,VALUEOUT>>(keyTypeInfo, valueTypleInfo);
	}

	/**
	 * Custom serialization methods.
	 *  @see http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		jobConf.write(out);
		out.writeObject(iterator);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		jobConf = new JobConf();
		jobConf.readFields(in);
		try {
			this.reducer = (Reducer) InstantiationUtil.instantiate(jobConf.getReducerClass());
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
