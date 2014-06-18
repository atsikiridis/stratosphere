package eu.stratosphere.hadoopcompatibility.mapred;

import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.operators.translation.TupleUnwrappingIterator;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.ResultTypeQueryable;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.WritableTypeInfo;
import eu.stratosphere.types.TypeInformation;
import eu.stratosphere.util.Collector;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class HadoopReduceGrouping<K extends WritableComparable,V extends Writable> extends GroupReduceFunction<Tuple2<K,V>, Tuple2<K,V>>  implements ResultTypeQueryable{

	private Class<K> keyoutClass;
	private Class<V> valueoutClass;
	private GroupReduceFunction<Tuple2<K,V>, Tuple2<K,V>> hadoopReducer;
	private Map<K, List<Tuple2<K,V>>> memo;
	private RawComparator<K> comparator;
	private JobConf jobConf;
	private ReducerTransformingIterator transformingIterator;

	public HadoopReduceGrouping(RawComparator<K> comparator, GroupReduceFunction<Tuple2<K,V>, Tuple2<K,V>> hadoopReducer, Class<K> keyClass, Class<V> valueClass) {
		this.keyoutClass = keyClass;
		this.valueoutClass = valueClass;
		this.hadoopReducer = hadoopReducer;
		this.comparator = comparator;
		this.memo = new HashMap<K, List<Tuple2<K, V>>>();
		this.jobConf = new JobConf();
		this.transformingIterator = new ReducerTransformingIterator();

	}


	private final class ReducerTransformingIterator extends TupleUnwrappingIterator<V,K>
			implements java.io.Serializable {

		private static final long serialVersionUID = 1L;
		private Iterator<Tuple2<K,V>> iterator;
		private K key;
		private Tuple2<K,V> first;

		@Override()
		public void set(Iterator<Tuple2<K,V>> iterator) {
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
		public V next() {
			if(this.first != null) {
				final V val = this.first.f1;
				this.first = null;
				return val;
			}
			final Tuple2<K,V> tuple = iterator.next();
			this.key = tuple.f0;
			return tuple.f1;
		}

		private K getKey() {
			return WritableUtils.clone(this.key, jobConf);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public TypeInformation<Tuple2<K,V>> getProducedType() {
		final WritableTypeInfo<K> keyTypeInfo = new WritableTypeInfo<K>(keyoutClass);
		final WritableTypeInfo<V> valueTypeInfo = new WritableTypeInfo<V>(valueoutClass);
		return new TupleTypeInfo<Tuple2<K,V>>(keyTypeInfo, valueTypeInfo);
	}

	@Override
	public void reduce(final Iterator<Tuple2<K, V>> values, final Collector<Tuple2<K, V>> out) throws Exception {
		transformingIterator.set(values);

		if (transformingIterator.hasNext()) {
			V firstValue = transformingIterator.next();
			K firstKey = transformingIterator.getKey();
			this.memo.put(firstKey, new ArrayList<Tuple2<K, V>>());
			this.memo.get(firstKey).add(new Tuple2<K, V>(firstKey, firstValue));
		}

		while (transformingIterator.hasNext()) {
			V hadoopValue = transformingIterator.next();
			K hadoopKey = transformingIterator.getKey();
			Iterator<K> it = this.memo.keySet().iterator();
			while (it.hasNext()){
				K key = it.next();
				if (comparator.compare(key, hadoopKey) == 0) {
					List<Tuple2<K,V>> curList =this.memo.get(key);
					if (curList != null) {
						curList.add(new Tuple2<K, V>(hadoopKey, hadoopValue));
						break;
					}
				}
				else if (! it.hasNext()) {
					this.memo.put(hadoopKey, new ArrayList<Tuple2<K, V>>());
					this.memo.get(hadoopKey).add(new Tuple2<K, V>(hadoopKey, hadoopValue));
					break;
				}
			}
		}

		for (final List<Tuple2<K, V>> tuple2s : this.memo.values()) {
			this.hadoopReducer.reduce(tuple2s.iterator(), out);
		}
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		//jobConf.setOutputValueGroupingComparator(org.apache.hadoop.io.WritableComparator.);
		jobConf.write(out);
		out.writeObject(keyoutClass);
		out.writeObject(valueoutClass);
		out.writeObject(hadoopReducer);
		out.writeObject(memo);
		out.writeObject(transformingIterator);

	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		jobConf = new JobConf();
		jobConf.readFields(in);
		keyoutClass = (Class<K>) in.readObject();
		valueoutClass = (Class<V>) in.readObject();
		try {
			this.comparator = org.apache.hadoop.io.WritableComparator.get(keyoutClass);
		//	this.comparator =  jobConf.getOutputValueGroupingComparator();
		}catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop grouping comparator", e);
		}
		ReflectionUtils.setConf(comparator, jobConf);
		hadoopReducer = (GroupReduceFunction) in.readObject();
		memo = (Map<K, List<Tuple2<K,V>>>) in.readObject();
		transformingIterator = (ReducerTransformingIterator) in.readObject();
		//valueoutClass = (Class<VALUEOUT>) in.readObject();
	}
}
