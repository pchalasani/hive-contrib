package com.yahoo.hive.contrib;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

/**
 * Hive Generic UDAF that collects keys and values into a map of the form [key , multiset(values)]
 * The multiset is represented by a hashmap {val -> count}.
 * 
 * @author harshars
 * 
 */
@Description(name = "segment", 
value = "_FUNC_(x,y) - collects keys and values into a map of the form [key , multiset(values)]",
extended = "Example: SELECT segment(k,v) FROM src;")
public class SegmentUDAF extends AbstractGenericUDAFResolver {

	static final Log log = LogFactory.getLog(SegmentUDAF.class);
	
	@Override
	public SegmentEvaluator getEvaluator(TypeInfo[] info)
			throws SemanticException {
		if (info.length != 2) {
			throw new UDFArgumentTypeException(info.length - 1,
					"Please specify exactly two arguments");
		}
		Category category1 = info[0].getCategory();
		if (!category1.equals(ObjectInspector.Category.PRIMITIVE)) {
			throw new UDFArgumentTypeException(1,
					"Only primitive type arguments are accepted but "
							+ info[0].getTypeName()
							+ " was passed as parameter.");
		}
		Category category2 = info[1].getCategory();
		if (!category2.equals(ObjectInspector.Category.PRIMITIVE)) {
			throw new UDFArgumentTypeException(1,
					"Only primitive type arguments are accepted but "
							+ info[1].getTypeName()
							+ " was passed as parameter.");
		}
		return new SegmentEvaluator();
	}
	
	public static class SegmentEvaluator extends GenericUDAFEvaluator {
		private PrimitiveObjectInspector  inputKeyOI;
		private ObjectInspector inputValOI;
		private StandardMapObjectInspector multisetValOI;
		private StandardMapObjectInspector mergeOI;
		private StandardMapObjectInspector partialOI;
		

		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			// init output object inspectors
			// The output of a partial aggregation is a list
			if (m == Mode.PARTIAL1) {
				inputKeyOI = (PrimitiveObjectInspector) parameters[0];
				inputValOI = parameters[1];
				WritableIntObjectInspector countOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
				multisetValOI = ObjectInspectorFactory.getStandardMapObjectInspector(inputValOI, countOI );
				mergeOI = ObjectInspectorFactory.getStandardMapObjectInspector(
						   ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI),
						   ObjectInspectorUtils.getStandardObjectInspector(multisetValOI) );
			} else {
				partialOI = (StandardMapObjectInspector) parameters[0];
				inputKeyOI = (PrimitiveObjectInspector) partialOI.getMapKeyObjectInspector();
				multisetValOI = (StandardMapObjectInspector) partialOI.getMapValueObjectInspector();
				inputValOI = multisetValOI.getMapKeyObjectInspector();
				mergeOI =  (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(partialOI);
			}
			return mergeOI;
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			AggregationBuffer buff= new SegmentAggBuffer();
			reset(buff);
			return buff;
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters)
				throws HiveException {
			Object k = parameters[0];
			Object v = parameters[1];

			if (k != null) {
				SegmentAggBuffer myagg = (SegmentAggBuffer) agg;
				put(k, v, myagg);
			}
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			SegmentAggBuffer myagg = (SegmentAggBuffer) agg;
			Map<Object,Object> partialResult = (HashMap<Object,Object>)  partialOI.getMap(partial);
			for(Object i : partialResult.keySet()) {
				merge(i, partialResult.get(i), myagg);
			}
		}

		@Override
		public void reset(AggregationBuffer buff) throws HiveException {
			SegmentAggBuffer arrayBuff = (SegmentAggBuffer) buff;
			arrayBuff.delegate = new HashMap<Object,Object>();
		}

		@Override
		public Object terminate(AggregationBuffer buffer) throws HiveException {
			SegmentAggBuffer aggBuffer = (SegmentAggBuffer) buffer;
			Map<Object,Object> result = new HashMap<Object,Object>(aggBuffer.delegate);
			return result;

		}

		private void put(Object key, Object val, SegmentAggBuffer buffer) {
			Object keyCopy = ObjectInspectorUtils.copyToStandardObject(key, this.inputKeyOI );
			Object valCopy = ObjectInspectorUtils.copyToStandardObject(val, this.inputValOI );
			Map<Object,IntWritable> multiset = (Map<Object, IntWritable>) buffer.delegate.get(keyCopy);
			if (multiset == null) {
				multiset = new HashMap<Object,IntWritable>();
				buffer.delegate.put(keyCopy, multiset);
			}
			IntWritable count = multiset.get(valCopy);
			if (count == null) {
				multiset.put(valCopy, new IntWritable(0));
			} else {
				count.set(count.get() + 1);
			}
		}

		private void merge(Object key, Object val, SegmentAggBuffer buffer) {
			Object keyCopy = ObjectInspectorUtils.copyToStandardObject(key, this.inputKeyOI );
			Map<Object, IntWritable> valCopy = (Map<Object, IntWritable>)this.multisetValOI.getMap(val);
			Map<Object,IntWritable> multiset = (Map<Object, IntWritable>) buffer.delegate.get(keyCopy);
			if (multiset == null) {
				multiset = new HashMap<Object,IntWritable>();
				buffer.delegate.put(keyCopy, multiset);
			}
			for (Map.Entry<Object, IntWritable> e : valCopy.entrySet()) {
				Object k = e.getKey();
				IntWritable v = multiset.get(k);
				if (v == null) {
					v = new IntWritable(0);
				}
				v.set(v.get() + e.getValue().get());
				multiset.put(k, v );
			}
		}

		@Override
		public Object terminatePartial(AggregationBuffer buffer) throws HiveException {
			SegmentAggBuffer aggBuffer = (SegmentAggBuffer) buffer;
			Map<Object,Object> ret = new HashMap<Object,Object>(aggBuffer.delegate);
			return ret;
		}
	}
	static class SegmentAggBuffer implements AggregationBuffer {
		Map<Object,Object> delegate = new HashMap<Object,Object>();
	}
}
