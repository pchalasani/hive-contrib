package com.yahoo.hive.contrib;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import com.yahoo.hive.contrib.MinHashUDAF.MinHashAggBuffer;
import com.yahoo.hive.contrib.MinHashUDAF.MinHashEvaluator;
import com.yahoo.streamlib.MinHash;

public class MinHashUDAFTest {

	@Test
	public void test() throws Exception{
		MinHashUDAF udaf = new MinHashUDAF();
		TypeInfo[] info = {TypeInfoFactory.stringTypeInfo};
		MinHashEvaluator evaluator = udaf.getEvaluator(info);
		ObjectInspector[] parameters = {
				PrimitiveObjectInspectorFactory.javaStringObjectInspector,
				PrimitiveObjectInspectorFactory.javaIntObjectInspector
		};
		ObjectInspector oi = evaluator.init(Mode.COMPLETE, parameters);
		assertTrue(oi instanceof BinaryObjectInspector);
		MinHashAggBuffer aggBuffer = evaluator.getNewAggregationBuffer();
		String [] data = {"A","B","C","A","B","A"};
		for(String datum : data) {
			evaluator.iterate(aggBuffer, new Object[]{datum, 32});
		}
		
		BytesWritable results = evaluator.terminate(aggBuffer);
		byte[] bytes = results.getBytes();
		byte[] serialized = new byte[results.getLength()];
		System.arraycopy(bytes, 0, serialized, 0, serialized.length);
		MinHash h = MinHash.fromBytes(serialized);
		assertFalse(h.isEmpty());
	}
}
