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

import com.yahoo.streamlib.ModHash;

@Description(name = "modhash", 
value = "_FUNC_(x, k) - computes the signature of a set " +
		"where 2^k = m is the modulus, returns a binary ",
extended = "Example: SELECT modhash(value,8) FROM src;")
public class ModHashUDAF extends AbstractGenericUDAFResolver{

	static final Log log = LogFactory.getLog(ModHashUDAF.class);
	
	@Override
	public ModHashEvaluator getEvaluator(TypeInfo[] info)
			throws SemanticException {
		if (info.length < 1 || info.length > 2) {
			throw new UDFArgumentTypeException(info.length - 1,
					"Please specify one to two arguments.");
		}
		Category category = info[0].getCategory();
		if (!category.equals(ObjectInspector.Category.PRIMITIVE)) {
			throw new UDFArgumentTypeException(1,
					"Only primitive type arguments are accepted but "
							+ info[0].getTypeName()
							+ " was passed as parameter.");
		}
		return new ModHashEvaluator();
	}
	
	public static class ModHashEvaluator extends GenericUDAFEvaluator {
		
		// input OI
		PrimitiveObjectInspector inputOI;
		IntObjectInspector kOI;

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
			} else {
				partialOI = (BinaryObjectInspector) parameters[0];
			}
			return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
		}
		
		@Override
		public ModHashAggBuffer getNewAggregationBuffer()
				throws HiveException {
			ModHashAggBuffer buffer = new ModHashAggBuffer();
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
			ModHashAggBuffer aggBuffer = (ModHashAggBuffer) buffer;
			
			if (!aggBuffer.initialized()) {
				int k = kOI.get(parameters[1]);
				aggBuffer.signature = new ModHash(k);
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
						ModHash other = ModHash.fromBytes(trimmedBytes);
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
				ModHashAggBuffer aggBuffer = (ModHashAggBuffer) buffer;
				byte[] bytes = new byte[bw.getLength()];
				System.arraycopy(bw.getBytes(), 0, bytes, 0, bw.getLength());

				ModHash other = ModHash.fromBytes(bytes);
				if (!aggBuffer.initialized()) {
					aggBuffer.signature = other.empty();
				}
				aggBuffer.signature = aggBuffer.signature.merge(other);
			}
		}

		@Override
		public void reset(AggregationBuffer buffer) throws HiveException {
			ModHashAggBuffer aggBuffer = (ModHashAggBuffer)buffer;
			aggBuffer.reset();
		}

		@Override
		public BytesWritable terminate(AggregationBuffer buffer) throws HiveException {
			ModHashAggBuffer aggBuffer = (ModHashAggBuffer) buffer;
			BytesWritable signature = new BytesWritable();
			byte [] b = aggBuffer.signature == null ? new byte[0] : aggBuffer.signature.toBytes();
			signature.set(b, 0, b.length);
			return signature;
		}

		@Override
		public Object terminatePartial(AggregationBuffer buffer)
				throws HiveException {
			ModHashAggBuffer aggBuffer = (ModHashAggBuffer) buffer;
			byte [] b = aggBuffer.signature == null ? new byte[0] :aggBuffer.signature.toBytes();
			partial.set(b, 0, b.length);
			return partial;
		}
	}
	static class ModHashAggBuffer implements AggregationBuffer {
		volatile ModHash signature ;
		
		public boolean initialized() {
			return signature != null;
		}
		
		public void reset() {
			if (signature != null) {
				signature = signature.empty();
			}
		}
	}
}


