package com.gigaspaces.persistency.qa.itest;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openspaces.itest.persistency.cassandra.data.MyCassandraPojo2;
import org.openspaces.itest.persistency.common.mock.MockOperationsBatchDataBuilder;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.document.SpaceDocument;

import edu.emory.mathcs.backport.java.util.Arrays;

@RunWith(Parameterized.class)
public class InitialDataLoadMongoTest extends AbstractMongoTest {
	private final Set<Object> writtenKeys = new HashSet<Object>();
	private final Set<Object> readKeys = new HashSet<Object>();
	private final String keyName = "key";

	private final KeyGenerator keyGenerator;
	private final int documentSize;
	private final int batchCount;
	private final int batchSize;
	//private final int batchLimit;

	public InitialDataLoadMongoTest(KeyGenerator keyGenerator,
			int documentSize, int batchCount, int batchSize, int batchLimit) {
		this.keyGenerator = keyGenerator;
		this.documentSize = documentSize;
		this.batchCount = batchCount;
		this.batchSize = batchSize;
		//this.batchLimit = batchLimit;
	}

	@SuppressWarnings("unchecked")
	@Parameters
	public static Collection<Object[]> data() {
		return Arrays
				.asList(new Object[][] {
						// test different keys with result set bigger than limit
						new Object[] { new StringKeyGenerator(), 10, 200, 200,
								9998 },
						new Object[] { new LongKeyGenerator(), 10, 200, 200,
								9999 },
						new Object[] { new DoubleKeyGenerator(), 10, 200, 200,
								10000 },
						new Object[] { new UUIDKeyGenerator(), 10, 200, 200,
								10001 },
						new Object[] { new BytesKeyGenerator(), 10, 200, 200,
								10002 },
						new Object[] { new DataClassKeyGenerator(), 10, 201,
								199, 10000 },
						// test big objects
						new Object[] { new UUIDKeyGenerator(),
								10 * 1024 * 1024, 10, 1, 4 } });
	}

	@Before
	public void before() {
		_syncInterceptor
				.onIntroduceType(createIntroduceTypeDataFromSpaceDocument(
						createSpaceDocument(false), keyName));
		_dataSource.initialMetadataLoad();
	}

	@Test
	public void test() {
		Date startWrite = new Date();
		for (int i = 0; i < batchCount; i++) {
			MockOperationsBatchDataBuilder builder = new MockOperationsBatchDataBuilder();
			for (int j = 0; j < batchSize; j++)
				builder.write(createSpaceDocument(true), keyName);
			_syncInterceptor.onOperationsBatchSynchronization(builder.build());
		}

		Date endWrite = new Date();
		Date startRead = new Date();
		DataIterator<Object> iterator = _dataSource.initialDataLoad();
		int count = 0;
		while (iterator.hasNext()) {
			count++;
			SpaceDocument spaceDoc = (SpaceDocument) iterator.next();
			Object key = spaceDoc.getProperty(keyName);
			readKeys.add(key);
		}

		iterator.close();

		Date endRead = new Date();

		double write = ((double) (endWrite.getTime() - startWrite.getTime()) / (double) 1000);
		double read = ((double) (endRead.getTime() - startRead.getTime()) / (double) 1000);
		System.err.println("write " + batchSize * batchCount + " documents in "
				+ write + " sec");
		System.err.println("read " + count + " documents in " + read + " sec");

		Assert.assertEquals("count differs", batchCount * batchSize, count);

		if (keyGenerator instanceof BytesKeyGenerator) {
			// kind of a hack, but what can you do
			return;
		}

		Assert.assertEquals("keys differ", writtenKeys, readKeys);
	}

	private SpaceDocument createSpaceDocument(boolean addToWrittenKeys) {
		Object key = keyGenerator.getKey();
		if (addToWrittenKeys)
			writtenKeys.add(key);
		byte[] bytes = new byte[documentSize];
		random.nextBytes(bytes);
		return new SpaceDocument("TypeName").setProperty(keyName, key)
				.setProperty("payload", bytes);
	}

	private static interface KeyGenerator {
		Object getKey();
	}

	private static class LongKeyGenerator implements KeyGenerator {
		public Object getKey() {
			return random.nextLong();
		}
	}

	private static class StringKeyGenerator implements KeyGenerator {
		public Object getKey() {
			return "#" + random.nextLong() + "#";
		}
	}

	private static class DoubleKeyGenerator implements KeyGenerator {
		public Object getKey() {
			return random.nextDouble();
		}
	}

	private static class UUIDKeyGenerator implements KeyGenerator {
		public Object getKey() {
			return UUID.randomUUID();
		}
	}

	private static class BytesKeyGenerator implements KeyGenerator {
		public Object getKey() {
			byte[] bytes = new byte[32];
			random.nextBytes(bytes);
			return bytes;
		}
	}

	private static class DataClassKeyGenerator implements KeyGenerator {
		public Object getKey() {
			return new MyCassandraPojo2("name", random.nextInt());
		}

	}

}
