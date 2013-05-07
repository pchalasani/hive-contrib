package com.yahoo.hive.contrib;

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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.streamlib.MinHash;

@Description(name = "minhash", 
value = "_FUNC_(x, k, n) - computes the signature of a set " +
		"where k = number of buckets and n = size of the universe, returns a binary ",
extended = "Example: SELECT minhash(value) FROM src;")
public class MinHashUDAF extends AbstractGenericUDAFResolver{

	static final Log log = LogFactory.getLog(MinHashUDAF.class);
	
	@Override
	public MinHashEvaluator getEvaluator(TypeInfo[] info)
			throws SemanticException {
		if (info.length < 1 || info.length > 3) {
			throw new UDFArgumentTypeException(info.length - 1,
					"Please specify one to three arguments.");
		}
		Category category = info[0].getCategory();
		if (!category.equals(ObjectInspector.Category.PRIMITIVE)) {
			throw new UDFArgumentTypeException(1,
					"Only primitive type arguments are accepted but "
							+ info[0].getTypeName()
							+ " was passed as parameter.");
		}
		return new MinHashEvaluator();
	}
	
	public static class MinHashEvaluator extends GenericUDAFEvaluator {
		
		// input OI
		PrimitiveObjectInspector inputOI;
		IntObjectInspector kOI;
		IntObjectInspector nOI;

		// intermediate results
		BinaryObjectInspector partialOI;
		
		BytesWritable partial = new BytesWritable();

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
				inputOI = (PrimitiveObjectInspector) parameters[0];
				kOI = (IntObjectInspector)parameters[1];
				nOI = (IntObjectInspector)parameters[2];
			} else {
				partialOI = (BinaryObjectInspector) parameters[0];
			}
			return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
		}
		
		@Override
		public MinHashAggBuffer getNewAggregationBuffer()
				throws HiveException {
			MinHashAggBuffer buffer = new MinHashAggBuffer();
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
			MinHashAggBuffer aggBuffer = (MinHashAggBuffer) buffer;
			
			if (!aggBuffer.initialized()) {
				int k = kOI.get(parameters[1]);
				int n = nOI.get(parameters[2]);
				aggBuffer.signature = new MinHash(k,n);
			}
			if (inputOI != null) {
				switch (inputOI.getPrimitiveCategory()) {
				case INT: {
					int v = ((IntObjectInspector) inputOI).get(obj);
					aggBuffer.signature.update(v);
					break;
				}
				case LONG: {
					long v = ((LongObjectInspector) inputOI).get(obj);
					aggBuffer.signature.update(v);
					break;
				}
				case STRING: {
					String v = ((StringObjectInspector) inputOI)
							.getPrimitiveJavaObject(obj);
					aggBuffer.signature.update(v);
					break;
				}
				case BINARY:
				default: {
					BytesWritable binary = PrimitiveObjectInspectorUtils
							.getBinary(obj, inputOI);
					byte[] bytes = binary.getBytes();
					if (inputOI instanceof BinaryObjectInspector) {
						byte[] trimmedBytes = new byte[binary.getLength()];
						System.arraycopy(bytes, 0, trimmedBytes, 0,
								trimmedBytes.length);
						MinHash other = MinHash.fromBytes(trimmedBytes);
						aggBuffer.signature = aggBuffer.signature.merge(other);
					} else {
						aggBuffer.signature.update(bytes);
					}
				}
				}
			}
				
		}

		@Override
		public void merge(AggregationBuffer buffer, Object obj)
				throws HiveException {
			if (obj != null) {
				BytesWritable bw = (BytesWritable) obj;
				MinHashAggBuffer aggBuffer = (MinHashAggBuffer) buffer;
				byte[] bytes = new byte[bw.getLength()];
				System.arraycopy(bw.getBytes(), 0, bytes, 0, bw.getLength());

				MinHash other = MinHash.fromBytes(bytes);
				if (!aggBuffer.initialized()) {
					aggBuffer.signature = new MinHash(other.getK(),other.getN());
				}
				aggBuffer.signature = aggBuffer.signature.merge(other);
			}
		}

		@Override
		public void reset(AggregationBuffer buffer) throws HiveException {
			((MinHashAggBuffer)buffer).signature = null;
		}

		@Override
		public BytesWritable terminate(AggregationBuffer buffer) throws HiveException {
			MinHashAggBuffer aggBuffer = (MinHashAggBuffer) buffer;
			BytesWritable signature = new BytesWritable();
			byte [] b = aggBuffer.signature.toBytes();
			signature.set(b, 0, b.length);
			return signature;
		}

		@Override
		public Object terminatePartial(AggregationBuffer buffer)
				throws HiveException {
			MinHashAggBuffer aggBuffer = (MinHashAggBuffer) buffer;
			byte [] b = aggBuffer.signature.toBytes();
			partial.set(b, 0, b.length);
			return partial;
		}
	}
	static class MinHashAggBuffer implements AggregationBuffer {
		MinHash signature ;
		
		public boolean initialized() {
			return signature != null;
		}
	}
}


