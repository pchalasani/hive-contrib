package com.yahoo.hive.contrib;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.eta.stats.cus.CountUniqueSketch;
import com.yahoo.eta.stats.cus.CountUniqueSketchSerialization;

/**
 * Hive Generic UDAF that computes an approximate distinct count, using a count
 * unique sketch.
 * 
 * @author harshars
 * 
 */
@Description(name = "approx_distinct", 
value = "_FUNC_(x) - x is either a serialized sketch, or an item to be distinct counted " +
		", returns a struct of the form (cardinality: _ , error: _ , sketch: _)",
extended = "Example: SELECT approx_distinct(values) FROM src;")
public class ApproxDistinctCountUDAF extends AbstractGenericUDAFResolver {

	static final Log log = LogFactory.getLog(ApproxDistinctCountUDAF.class);

	@Override
	public SketchEvaluator getEvaluator(TypeInfo[] info)
			throws SemanticException {
		if (info.length != 1) {
			throw new UDFArgumentTypeException(info.length - 1,
					"Please specify exactly one argument.");
		}
		Category category = info[0].getCategory();
		if (!category.equals(ObjectInspector.Category.PRIMITIVE)) {
			throw new UDFArgumentTypeException(1,
					"Only primitive type arguments are accepted but "
							+ info[0].getTypeName()
							+ " was passed as parameter.");
		}
		return new SketchEvaluator();
	}

	public static class SketchEvaluator extends GenericUDAFEvaluator {

		// input OI
		PrimitiveObjectInspector inputOI;

		// intermediate results
		BinaryObjectInspector partialOI;

		BytesWritable partial = new BytesWritable();
		
		static final String CARDINALITY = "cardinality";
		static final String ERROR = "error";
		static final String SKETCH = "sketch";

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			System.out.println("Init : " + m);
			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
				assert (parameters.length == 1);
				inputOI = (PrimitiveObjectInspector) parameters[0];
			} else {
				partialOI = (BinaryObjectInspector) parameters[0];
			}

			if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
				return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
			} else {
				ArrayList<String> fNames = new ArrayList<String>();
                fNames.add(CARDINALITY);
                fNames.add(ERROR);
                fNames.add(SKETCH);
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
                return ObjectInspectorFactory.getStandardStructObjectInspector(fNames, foi);
			}
		}

		@Override
		public ApproxDistinctCountAggBuffer getNewAggregationBuffer()
				throws HiveException {
			ApproxDistinctCountAggBuffer buffer = new ApproxDistinctCountAggBuffer();
			reset(buffer);
			return buffer;
		}

		@Override
		public void iterate(AggregationBuffer buffer, Object[] parameters)
				throws HiveException {
			if (parameters[0] == null) {
				return;
			}
			Object obj = parameters[0];
			ApproxDistinctCountAggBuffer aggBuffer = (ApproxDistinctCountAggBuffer) buffer;
			if (inputOI != null) {
				BytesWritable binary = PrimitiveObjectInspectorUtils.getBinary(
						obj, inputOI);
				byte[] bytes = binary.getBytes();
				if (inputOI instanceof BinaryObjectInspector) {
					byte[] trimmedBytes = new byte[binary.getLength()];
					System.arraycopy(bytes, 0, trimmedBytes, 0, trimmedBytes.length);
					CountUniqueSketch other = CountUniqueSketchSerialization.deserializeSketch(trimmedBytes);
					aggBuffer.sketch = aggBuffer.sketch.merge(other);
				} else {
					aggBuffer.sketch.update(bytes);
				}
				
			}
		}

		@Override
		public void merge(AggregationBuffer buffer, Object obj)
				throws HiveException {
			if (obj != null) {
				BytesWritable bw = (BytesWritable) obj;
				ApproxDistinctCountAggBuffer aggBuffer = (ApproxDistinctCountAggBuffer) buffer;
				byte[] bytes = new byte[bw.getLength()];
				System.arraycopy(bw.getBytes(), 0, bytes, 0, bw.getLength());

				CountUniqueSketch other = CountUniqueSketchSerialization
						.deserializeSketch(bytes);
				aggBuffer.sketch = aggBuffer.sketch.merge(other);
			}
		}

		@Override
		public void reset(AggregationBuffer buffer) throws HiveException {
			((ApproxDistinctCountAggBuffer) buffer).sketch = new CountUniqueSketch();
		}

		@Override
		public ArrayList<Object> terminate(AggregationBuffer buffer)
				throws HiveException {
			ApproxDistinctCountAggBuffer aggBuffer = (ApproxDistinctCountAggBuffer) buffer;
			if (aggBuffer.sketch.isEmpty()) {
				return null;
			} else {
				DoubleWritable cardinality = new DoubleWritable();
				cardinality.set(aggBuffer.sketch.getInverseEstimate());
				DoubleWritable error = new DoubleWritable();
				error.set(aggBuffer.sketch.getVariance());
				BytesWritable sketch = new BytesWritable();
				byte[] serializedSketch = CountUniqueSketchSerialization.serializeSketch(aggBuffer.sketch);
				sketch.set(serializedSketch,0,serializedSketch.length);
				ArrayList<Object> results = new ArrayList<Object>();
				results.add(cardinality);
				results.add(error);
				results.add(sketch);
				return results;
			}
		}

		@Override
		public Object terminatePartial(AggregationBuffer buffer)
				throws HiveException {
			ApproxDistinctCountAggBuffer aggBuffer = (ApproxDistinctCountAggBuffer) buffer;
			byte[] s = CountUniqueSketchSerialization
					.serializeSketch(aggBuffer.sketch);
			partial.set(s, 0, s.length);
			return partial;
		}

	}

	static class ApproxDistinctCountAggBuffer implements AggregationBuffer {

		CountUniqueSketch sketch;

	}
}
