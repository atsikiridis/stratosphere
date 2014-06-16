package eu.stratosphere.hadoopcompatibility.mapred;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;

public class GroupComparisonFlatMap<K,V> extends MapFunction<Tuple2<Integer,Tuple2<K,V>>,Tuple2<K,V> > {


	@Override
	public Tuple2<K, V> map(final Tuple2<Integer, Tuple2<K, V>> value) throws Exception {
		return value.f1;
	}
}
