package com.yahoo.hive.contrib;

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import com.yahoo.hive.contrib.OverlapUDAF.OverlapAggBuffer;
import com.yahoo.hive.contrib.OverlapUDAF.OverlapEvaluator;
import com.yahoo.streamlib.MinHash;

public class OverlapUDAFTest {

	@Test
	public void test() throws Exception{
		OverlapUDAF udaf = new OverlapUDAF();
		TypeInfo [] infos = {
				TypeInfoFactory.binaryTypeInfo,
				TypeInfoFactory.binaryTypeInfo
		};
		OverlapEvaluator evaluator = udaf.getEvaluator(infos);
		ObjectInspector[] parameters = {
				PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
				PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
		};
		ObjectInspector oi = evaluator.init(Mode.COMPLETE, parameters);
		assertTrue(oi instanceof DoubleObjectInspector);
		OverlapAggBuffer aggBuffer = evaluator.getNewAggregationBuffer();
		evaluator.iterate(aggBuffer, new Object[] {
				newMinHash("A","B","D","J"),
				newMinHash("B","E","D")
		});
		evaluator.terminate(aggBuffer);
		double jaccardSimilarity = aggBuffer.xsignature.jaccardSimilarity(aggBuffer.ysignature);
		assertTrue(jaccardSimilarity < 0.4);
	}

	@Test
	public void test2() throws Exception{
		OverlapUDAF udaf = new OverlapUDAF();
		TypeInfo [] infos = {
				TypeInfoFactory.binaryTypeInfo,
				TypeInfoFactory.binaryTypeInfo
		};
		OverlapEvaluator evaluator = udaf.getEvaluator(infos);
		ObjectInspector[] parameters = {
				PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
				PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
		};
		ObjectInspector oi = evaluator.init(Mode.COMPLETE, parameters);
		assertTrue(oi instanceof DoubleObjectInspector);
		OverlapAggBuffer aggBuffer = evaluator.getNewAggregationBuffer();
		Set<String> x = new HashSet<String>();
		Set<String> y = new HashSet<String>();
		
		for (int i = 0; i< 1000 ; i++) {
			x.add(i + "0"); x.add(i + "1"); x.add(i +  "2");
			y.add(i + "1"); y.add(i + "2"); y.add(i +  "3");
			evaluator.iterate(aggBuffer, new Object[] {
					newMinHash(i + "0", i + "1", i + "2"),
					newMinHash(i + "1", i + "2", i + "3")
			});
		}
		
		evaluator.terminate(aggBuffer);
		double jaccardSimilarity = aggBuffer.xsignature.jaccardSimilarity(aggBuffer.ysignature);
		assertTrue(jaccardSimilarity >= 0.5);
	}
	
	private BytesWritable newMinHash(String... data) {
		MinHash h = new MinHash();
		for(String datum : data) {
			h.update(datum);
		}
		BytesWritable b = new BytesWritable();
		byte [] bytes = h.toBytes();
		b.set(bytes, 0, bytes.length);
		return b;
	}
}
