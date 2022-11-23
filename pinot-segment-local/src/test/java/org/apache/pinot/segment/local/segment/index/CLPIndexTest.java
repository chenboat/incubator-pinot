package org.apache.pinot.segment.local.segment.index;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.realtime.impl.json.MutableJsonIndexImpl;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OffHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OnHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.json.ImmutableJsonIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class CLPIndexTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "CLPIndexTest");

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }


  @Test
  public void testCLPIndex()
      throws Exception {
    String[] records = new String[]{
        "Uploading a segment %s to table: %s , push type URI, (Derived from segment metadata)"
    };
    //CHECKSTYLE:ON
    // @formatter: on

    String onHeapColumnName = "onHeap";
    try (CLPIndexCreator clpIndexCreator = new CLPIndexCreator(INDEX_DIR, onHeapColumnName)) {
      for (String record : records) {
        clpIndexCreator.add(record);
      }
      clpIndexCreator.seal();
    }
    File onHeapIndexFile = new File(INDEX_DIR, onHeapColumnName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(onHeapIndexFile.exists());

    String offHeapColumnName = "offHeap";
    try (JsonIndexCreator offHeapIndexCreator = new OffHeapJsonIndexCreator(INDEX_DIR, offHeapColumnName)) {
      for (String record : records) {
        offHeapIndexCreator.add(record);
      }
      offHeapIndexCreator.seal();
    }
    File offHeapIndexFile = new File(INDEX_DIR, offHeapColumnName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(offHeapIndexFile.exists());

    try (PinotDataBuffer onHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(onHeapIndexFile);
        PinotDataBuffer offHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(offHeapIndexFile);
        JsonIndexReader onHeapIndexReader = new ImmutableJsonIndexReader(onHeapDataBuffer, records.length);
        JsonIndexReader offHeapIndexReader = new ImmutableJsonIndexReader(offHeapDataBuffer, records.length);
        MutableJsonIndexImpl mutableJsonIndex = new MutableJsonIndexImpl()) {
      for (String record : records) {
        mutableJsonIndex.add(record);
      }
      JsonIndexReader[] indexReaders = new JsonIndexReader[]{onHeapIndexReader, offHeapIndexReader, mutableJsonIndex};
      for (JsonIndexReader indexReader : indexReaders) {
        MutableRoaringBitmap matchingDocIds = getMatchingDocIds(indexReader, "name='bob'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{1});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].street\" = 'street-21'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{2});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].street\" NOT IN ('street-10', 'street-22')");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[0].country\" IN ('ca', 'us')");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].types[1]\" = 'office'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[0].types[0]\" = 'home'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[1].types[*]\" = 'home'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[0]);

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].types[*]\" IS NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 2});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[1].types[*]\" IS NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 2, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "abc IS NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 2, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"skills[*]\" IS NOT NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 2});

        matchingDocIds =
            getMatchingDocIds(indexReader, "\"addresses[*].country\" = 'ca' AND \"skills[*]\" IS NOT NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].country\" = 'us' OR \"skills[*]\" IS NOT NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 2});
      }
    }
  }

  private MutableRoaringBitmap getMatchingDocIds(JsonIndexReader indexReader, String filter) {
    return indexReader.getMatchingDocIds(filter);
  }
}
