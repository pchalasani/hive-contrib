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
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;

import com.google.common.primitives.Ints;
import com.yahoo.streamlib.MinHash;

@Description(name = "similarity", value = "_FUNC_(x) - computes the similarity between two sets "
		+ ", using the Jaccard Index computed via min hash ", extended = "Example: SELECT overlap(x,y) FROM src;")
public class SimilarityUDAF extends AbstractGenericUDAFResolver {

	static final Log log = LogFactory.getLog(SimilarityUDAF.class);

	@Override
	public OverlapEvaluator getEvaluator(TypeInfo[] info)
			throws SemanticException {
		if (info.length != 2) {
			throw new UDFArgumentTypeException(info.length - 1,
					"Please specify exactly two arguments.");
		}
		Category category = info[0].getCategory();
		if (!category.equals(ObjectInspector.Category.PRIMITIVE)) {
			throw new UDFArgumentTypeException(1,
					"Only primitive type arguments are accepted but "
							+ info[0].getTypeName()
							+ " was passed as parameter.");
		}
		Category category2 = info[1].getCategory();
		if (!category2.equals(ObjectInspector.Category.PRIMITIVE)) {
			throw new UDFArgumentTypeException(1,
					"Only primitive type arguments are accepted but "
							+ info[0].getTypeName()
							+ " was passed as parameter.");
		}
		return new OverlapEvaluator();
	}

	public static class OverlapEvaluator extends GenericUDAFEvaluator {

		// input OI
		BinaryObjectInspector inputOIx;
		BinaryObjectInspector inputOIy;

		// intermediate results
		WritableBinaryObjectInspector partialOI;

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			partialOI = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
				assert (parameters.length == 2);
				inputOIx = (BinaryObjectInspector) parameters[0];
				inputOIy = (BinaryObjectInspector) parameters[1];
				return m == Mode.PARTIAL1 ? partialOI
						: PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
			} else {
				if (m == Mode.PARTIAL2) {
					return partialOI;
				} else {
					return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
				}

			}
		}

		@Override
		public OverlapAggBuffer getNewAggregationBuffer() throws HiveException {
			OverlapAggBuffer buffer = new OverlapAggBuffer();
			reset(buffer);
			return buffer;
		}

		@Override
		public void iterate(AggregationBuffer buffer, Object[] parameters)
				throws HiveException {
			BytesWritable xbinary = PrimitiveObjectInspectorUtils.getBinary(
					parameters[0], inputOIx);

			OverlapAggBuffer aggBuffer = (OverlapAggBuffer) buffer;
			MinHash xSignature = fromBytes(xbinary);
			if (!aggBuffer.initialized()) {
				aggBuffer.xsignature = new MinHash(xSignature.getK());
				aggBuffer.ysignature = new MinHash(xSignature.getK());
			}
			aggBuffer.xsignature = aggBuffer.xsignature
					.merge(xSignature);
			BytesWritable ybinary = PrimitiveObjectInspectorUtils.getBinary(
					parameters[1], inputOIy);
			aggBuffer.ysignature = aggBuffer.ysignature
					.merge(fromBytes(ybinary));
		}

		private MinHash fromBytes(BytesWritable binary) {
			byte[] xbytes = binary.getBytes();
			byte[] trimmedBytes = new byte[binary.getLength()];
			System.arraycopy(xbytes, 0, trimmedBytes, 0, trimmedBytes.length);
			return MinHash.fromBytes(trimmedBytes);
		}

		@Override
		public void merge(AggregationBuffer buffer, Object obj)
				throws HiveException {
			if (obj != null) {
				OverlapAggBuffer aggBuffer = (OverlapAggBuffer) buffer;
				BytesWritable bw = (BytesWritable) obj;
				byte[] bytes = new byte[bw.getLength()];
				System.arraycopy(bw.getBytes(), 0, bytes, 0, bw.getLength());
				int xLength = Ints.fromBytes(bytes[0], bytes[1], bytes[2],
						bytes[3]);
				int yLength = Ints.fromBytes(bytes[4], bytes[5], bytes[6],
						bytes[7]);
				byte[] xBytes = new byte[xLength];
				System.arraycopy(bytes, 8, xBytes, 0, xLength);
				byte[] yBytes = new byte[yLength];
				System.arraycopy(bytes, 8 + xLength, yBytes, 0, yLength);
				MinHash xSignature = MinHash.fromBytes(xBytes);
				if (!aggBuffer.initialized()) {
					aggBuffer.xsignature = new MinHash(xSignature.getK());
					aggBuffer.ysignature = new MinHash(xSignature.getK());
				}
				
				aggBuffer.xsignature = aggBuffer.xsignature.merge(xSignature);
				aggBuffer.ysignature = aggBuffer.ysignature.merge(MinHash
						.fromBytes(yBytes));
			}
		}

		@Override
		public void reset(AggregationBuffer buffer) throws HiveException {
			OverlapAggBuffer aggBuffer = (OverlapAggBuffer) buffer;
			aggBuffer.reset();
		}

		@Override
		public Object terminate(AggregationBuffer buffer) throws HiveException {
			OverlapAggBuffer aggBuffer = (OverlapAggBuffer) buffer;
			MinHash x = aggBuffer.xsignature;
			MinHash y = aggBuffer.ysignature;
			double jaccard = x.jaccardSimilarity(y);
			DoubleWritable w = new DoubleWritable(jaccard);
			return w;
		}

		@Override
		public Object terminatePartial(AggregationBuffer buffer)
				throws HiveException {
			OverlapAggBuffer aggBuffer = (OverlapAggBuffer) buffer;
			byte[] xBytes = aggBuffer.xsignature.toBytes();
			byte[] yBytes = aggBuffer.ysignature.toBytes();
			byte[] bytes = new byte[xBytes.length + yBytes.length + 8];
			// first 4 bytes is the length of x buffer, next four is length of y
			// buffer.
			byte[] xLength = Ints.toByteArray(xBytes.length);
			System.arraycopy(xLength, 0, bytes, 0, 4);
			byte[] yLength = Ints.toByteArray(yBytes.length);
			System.arraycopy(yLength, 0, bytes, 4, 4);
			System.arraycopy(xBytes, 0, bytes, 8, xBytes.length);
			System.arraycopy(yBytes, 0, bytes, 8 + xBytes.length, yBytes.length);
			BytesWritable partial = new BytesWritable();
			partial.set(bytes, 0, bytes.length);
			return partial;
		}

	}

	static class OverlapAggBuffer implements AggregationBuffer {
		MinHash xsignature ;
		MinHash ysignature ;
		
		public boolean initialized() {
			return xsignature != null && ysignature !=null;
		}

		public void reset() {
			if (initialized()) {
				xsignature = xsignature.empty();
				ysignature = xsignature.empty();
			}
		}
	}
}
