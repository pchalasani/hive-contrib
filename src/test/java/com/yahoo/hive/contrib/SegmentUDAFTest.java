package com.yahoo.hive.contrib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import com.yahoo.hive.contrib.SegmentUDAF.SegmentAggBuffer;
import com.yahoo.hive.contrib.SegmentUDAF.SegmentEvaluator;

public class SegmentUDAFTest {

	@Test
	public void testPartial1Complete() throws Exception{
		SegmentUDAF udaf = new SegmentUDAF();
		TypeInfo[] info = {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.intTypeInfo};
		SegmentEvaluator evaluator = udaf.getEvaluator(info);
		ObjectInspector[] parameters = {PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaIntObjectInspector};
		ObjectInspector oi = evaluator.init(Mode.PARTIAL1, parameters);
		assertTrue(oi instanceof MapObjectInspector);
		SegmentAggBuffer aggBuffer = evaluator.getNewAggregationBuffer();
		String [] keys = {"foo","bar","baz"};
		for (String key : keys) {
			for (int i = 0; i < 10; i++) {
				evaluator.iterate(aggBuffer, new Object[] { key, 1 });
			}
		}
		
		evaluator.init(Mode.COMPLETE, new ObjectInspector[]{oi});
		Map<String, Map<Integer, Integer>> result = (Map<String, Map<Integer, Integer>>) evaluator.terminate(aggBuffer);
		assertTrue(result.size() == 3);
		
	}

	@Test
	public void testPartial1MergeComplete() throws Exception{
		SegmentUDAF udaf = new SegmentUDAF();
		TypeInfo[] info = {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.intTypeInfo};
		SegmentEvaluator evaluator = udaf.getEvaluator(info);
		ObjectInspector[] parameters = {PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaIntObjectInspector};
		ObjectInspector oi = evaluator.init(Mode.PARTIAL1, parameters);
		assertTrue(oi instanceof MapObjectInspector);
		SegmentAggBuffer aggBuffer = evaluator.getNewAggregationBuffer();
		String [] keys = {"foo","bar","baz"};
		for (String key : keys) {
			for (int i = 0; i < 10; i++) {
				evaluator.iterate(aggBuffer, new Object[] { key, 1 });
			}
		}
		
		Object partial = evaluator.terminatePartial(aggBuffer);
		SegmentAggBuffer aggBuffer2 = evaluator.getNewAggregationBuffer();
		for (String key : keys) {
			for (int i = 0; i < 10; i++) {
				evaluator.iterate(aggBuffer2, new Object[] { key, 1 });
			}
		}
		evaluator.init(Mode.COMPLETE, new ObjectInspector[]{oi});
		evaluator.merge(aggBuffer2, partial);
		Map<String, Map<Integer, Integer>> result = (Map<String, Map<Integer, Integer>>) evaluator.terminate(aggBuffer2);
		System.out.println(result);
		assertTrue(result.size() == 3);
		assertEquals(result.get("bar").get(1), new IntWritable(20));
		
	}

}
