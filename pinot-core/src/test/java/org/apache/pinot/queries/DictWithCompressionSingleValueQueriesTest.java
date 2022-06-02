/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.queries;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * The <code>DictionaryWithCompressionSingleValueQueriesTest</code> class sets up the index segment for the dictionary
 * with compression single-value queries test.
 * <p>There are totally 18 columns, 30000 records inside the original Avro file where 11 columns are selected to build
 * the index segment. Selected columns information are as following:
 * <ul>
 *   ColumnName, FieldType, DataType, Cardinality, IsSorted, HasInvertedIndex, HasDictionaryWithCompression: S1, S2
 *   <li>column1, METRIC, INT, 6582, F, F, F, F</li>
 *   <li>column3, METRIC, INT, 21910, F, F, F, F</li>
 *   <li>column5, DIMENSION, STRING, 1, T, F, T, T</li>
 *   <li>column6, DIMENSION, INT, 608, F, T, T, T</li>
 *   <li>column7, DIMENSION, INT, 146, F, T, F, F</li>
 *   <li>column9, DIMENSION, INT, 1737, F, F, T, F</li>
 *   <li>column11, DIMENSION, STRING, 5, F, T, T, F</li>
 *   <li>column12, DIMENSION, STRING, 5, F, F, F, F</li>
 *   <li>column17, METRIC, INT, 24, F, T, F, F</li>
 *   <li>column18, METRIC, INT, 1440, F, T, F, F</li>
 *   <li>daysSinceEpoch, TIME, INT, 2, T, F, F, F</li>
 * </ul>
 */
public class DictWithCompressionSingleValueQueriesTest extends BaseQueriesTest {
  private static final String AVRO_DATA = "data" + File.separator + "test_data-sv.avro";
  private static final String SEGMENT_NAME_1 = "testTable_126164076_167572854";
  private static final String SEGMENT_NAME_2 = "testTable_126164076_167572855";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(),
      "DictWithCompressionSingleValueQueriesTest");

  private static final String SELECT_STAR_QUERY = "SELECT * FROM testTable";
  private static final String SELECTION_QUERY = "SELECT column1, column5, column6, column9, column11 FROM testTable";

  // Hard-coded query filter.
  private static final String FILTER = " WHERE column1 > 100000000"
      + " AND column3 BETWEEN 20000000 AND 1000000000"
      + " AND column5 = 'gFuH'"
      + " AND (column6 < 500000000 OR column11 NOT IN ('t', 'P'))"
      + " AND daysSinceEpoch = 126164076";

  private static final DataSchema DATA_SCHEMA = new DataSchema(new String[]{"column1", "column11", "column12",
      "column17", "column18", "column3", "column5", "column6", "column7", "column9", "daysSinceEpoch"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
          DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT,
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});

  private static final DataSchema DATA_SCHEMA_SELECTION = new DataSchema(new String[]{"column1", "column5", "column6",
      "column9", "column11"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

  private IndexSegment _indexSegment;
  // Contains 2 index segments, one with 4 columns with dictionary with compression enabled, and the other with just 2.
  private List<IndexSegment> _indexSegments;

  @BeforeTest
  public void buildSegment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Get resource file path.
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String filePath = resource.getFile();

    // Build the segment schema.
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addMetric("column1", FieldSpec.DataType.INT)
        .addMetric("column3", FieldSpec.DataType.INT).addSingleValueDimension("column5", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column6", FieldSpec.DataType.INT)
        .addSingleValueDimension("column7", FieldSpec.DataType.INT)
        .addSingleValueDimension("column9", FieldSpec.DataType.INT)
        .addSingleValueDimension("column11", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column12", FieldSpec.DataType.STRING).addMetric("column17", FieldSpec.DataType.INT)
        .addMetric("column18", FieldSpec.DataType.INT)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"), null).build();

    createSegment(filePath, SEGMENT_NAME_1, schema);
    createSegment(filePath, SEGMENT_NAME_2, schema);
  }

  private void createSegment(String filePath, String segmentName, Schema schema)
      throws Exception {
    // Create field configs for the dictionary with compression columns
    List<FieldConfig> fieldConfigList = new ArrayList<>();
    fieldConfigList.add(new FieldConfig("column5", FieldConfig.EncodingType.RAWDICTIONARY, Collections.singletonList(
        FieldConfig.IndexType.FORWARDDICTCOMPRESSED), FieldConfig.CompressionCodec.LZ4, Collections.emptyMap()));
    fieldConfigList.add(new FieldConfig("column6", FieldConfig.EncodingType.RAWDICTIONARY, Collections.singletonList(
        FieldConfig.IndexType.FORWARDDICTCOMPRESSED), FieldConfig.CompressionCodec.LZ4, Collections.emptyMap()));
    if (segmentName.equals(SEGMENT_NAME_1)) {
      fieldConfigList.add(new FieldConfig("column9", FieldConfig.EncodingType.RAWDICTIONARY,
          Collections.singletonList(FieldConfig.IndexType.FORWARDDICTCOMPRESSED), FieldConfig.CompressionCodec.SNAPPY,
          Collections.emptyMap()));
      fieldConfigList.add(new FieldConfig("column11", FieldConfig.EncodingType.RAWDICTIONARY,
          Collections.singletonList(FieldConfig.IndexType.FORWARDDICTCOMPRESSED), FieldConfig.CompressionCodec.SNAPPY,
          Collections.emptyMap()));
    }

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("daysSinceEpoch")
            .setFieldConfigList(fieldConfigList).build();

    // Create the segment generator config.
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName("testTable");
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setSegmentName(segmentName);
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    segmentGeneratorConfig.setSkipTimeValueCheck(true);
    segmentGeneratorConfig.setInvertedIndexCreationColumns(
        Arrays.asList("column6", "column7", "column11", "column17", "column18"));

    Map<String, ChunkCompressionType> dictionaryWithCompressionColumnMap = new HashMap<>();
    dictionaryWithCompressionColumnMap.put("column5", ChunkCompressionType.LZ4);
    dictionaryWithCompressionColumnMap.put("column6", ChunkCompressionType.LZ4);
    if (segmentName.equals(SEGMENT_NAME_1)) {
      dictionaryWithCompressionColumnMap.put("column9", ChunkCompressionType.SNAPPY);
      dictionaryWithCompressionColumnMap.put("column11", ChunkCompressionType.SNAPPY);
    }
    segmentGeneratorConfig.setDictionaryCompressionForwardIndexCompressionType(dictionaryWithCompressionColumnMap);

    // Build the index segment.
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();
  }

  @BeforeClass
  public void loadSegment()
      throws Exception {
    ImmutableSegment immutableSegment1 = loadSegmentWithMetadataChecks(SEGMENT_NAME_1);
    ImmutableSegment immutableSegment2 = loadSegmentWithMetadataChecks(SEGMENT_NAME_2);

    _indexSegment = immutableSegment1;
    _indexSegments = Arrays.asList(immutableSegment1, immutableSegment2);
  }

  private ImmutableSegment loadSegmentWithMetadataChecks(String segmentName)
      throws Exception {
    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName),
        ReadMode.heap);

    Map<String, ColumnMetadata> columnMetadataMap1 = immutableSegment.getSegmentMetadata().getColumnMetadataMap();
    columnMetadataMap1.forEach((column, metadata) -> {
      if (column.equals("column5") || column.equals("column6")) {
        assertTrue(metadata.hasDictionary());
        assertTrue(metadata.hasDictionaryWithCompression());
      } else if (segmentName.equals(SEGMENT_NAME_1) && (column.equals("column9")
          || column.equals("column11"))) {
        assertTrue(metadata.hasDictionary());
        assertTrue(metadata.hasDictionaryWithCompression());
      } else {
        assertFalse(metadata.hasDictionaryWithCompression());
      }
    });

    return immutableSegment;
  }

  @AfterClass
  public void destroySegment() {
    _indexSegments.forEach((IndexSegment::destroy));
  }

  @AfterTest
  public void deleteSegment() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Override
  protected String getFilter() {
    return FILTER;
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @Test
  public void testSelectStarQuery() {
    // Select * without any filters
    BrokerResponseNative brokerResponseNative = getBrokerResponse(SELECT_STAR_QUERY);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 40L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 440L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), DATA_SCHEMA);
    List<Object[]> resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 11);
      assertEquals((String) resultRow[6], "gFuH");
    }

    Object[] firstRow = resultRows.get(0);
    // Column 1
    assertEquals(((Integer) firstRow[0]).intValue(), 1578964907);
    // Column 11
    assertEquals((String) firstRow[1], "P");

    // Validate that results returned for each segment (especially for columns which have different dictionary with
    // compression flags) are the same
    BaseOperator<IntermediateResultsBlock> selectionOperator1 = getOperatorForSegment(SELECT_STAR_QUERY,
        _indexSegments.get(0));
    IntermediateResultsBlock resultsBlock1 = selectionOperator1.nextBlock();
    List<Object[]> selectionResult1 = (List<Object[]>) resultsBlock1.getSelectionResult();

    BaseOperator<IntermediateResultsBlock> selectionOperator2 = getOperatorForSegment(SELECT_STAR_QUERY,
        _indexSegments.get(1));
    IntermediateResultsBlock resultsBlock2 = selectionOperator2.nextBlock();
    List<Object[]> selectionResult2 = (List<Object[]>) resultsBlock2.getSelectionResult();

    assertNotNull(selectionResult1);
    assertNotNull(selectionResult2);
    assertEquals(selectionResult1.size(), selectionResult2.size());
    for (int i = 0; i < selectionResult1.size(); i++) {
      Object[] rowQuery1 = selectionResult1.get(i);
      Object[] rowQuery2 = selectionResult2.get(i);

      assertEquals(rowQuery1.length, rowQuery2.length);
      for (int j = 0; j < rowQuery1.length; j++) {
        assertEquals(rowQuery1[j], rowQuery2[j]);
      }
    }

    // Select * with filters
    brokerResponseNative = getBrokerResponse(SELECT_STAR_QUERY + FILTER);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 40L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 440L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 192964L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), DATA_SCHEMA);
    resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 11);
      assertEquals((String) resultRow[6], "gFuH");
    }

    firstRow = resultRows.get(0);
    // Column 1
    assertEquals(((Integer) firstRow[0]).intValue(), 351823652);
    // Column 11
    assertEquals((String) firstRow[1], "t");

    // Validate that results returned for each segment (especially for columns which have different dictionary with
    // compression flags) are the same
    selectionOperator1 = getOperatorForSegment(SELECT_STAR_QUERY + FILTER, _indexSegments.get(0));
    resultsBlock1 = selectionOperator1.nextBlock();
    selectionResult1 = (List<Object[]>) resultsBlock1.getSelectionResult();

    selectionOperator2 = getOperatorForSegment(SELECT_STAR_QUERY + FILTER, _indexSegments.get(1));
    resultsBlock2 = selectionOperator2.nextBlock();
    selectionResult2 = (List<Object[]>) resultsBlock2.getSelectionResult();

    assertNotNull(selectionResult1);
    assertNotNull(selectionResult2);
    assertEquals(selectionResult1.size(), selectionResult2.size());
    for (int i = 0; i < selectionResult1.size(); i++) {
      Object[] rowQuery1 = selectionResult1.get(i);
      Object[] rowQuery2 = selectionResult2.get(i);

      assertEquals(rowQuery1.length, rowQuery2.length);
      for (int j = 0; j < rowQuery1.length; j++) {
        assertEquals(rowQuery1[j], rowQuery2[j]);
      }
    }
  }

  @Test
  public void testSelectColumnsQuery() {
    // Select columns without any filters
    BrokerResponseNative brokerResponseNative = getBrokerResponse(SELECTION_QUERY);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 40L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 200L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), DATA_SCHEMA_SELECTION);
    List<Object[]> resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 5);
      assertEquals((String) resultRow[1], "gFuH");
    }

    Object[] firstRow = resultRows.get(0);
    // Column 1
    assertEquals(((Integer) firstRow[0]).intValue(), 1578964907);
    // Column 11
    assertEquals((String) firstRow[4], "P");

    // Validate that results returned for each segment (especially for columns which have different dictionary with
    // compression flags) are the same
    BaseOperator<IntermediateResultsBlock> selectionOperator1 = getOperatorForSegment(SELECTION_QUERY,
        _indexSegments.get(0));
    IntermediateResultsBlock resultsBlock1 = selectionOperator1.nextBlock();
    List<Object[]> selectionResult1 = (List<Object[]>) resultsBlock1.getSelectionResult();

    BaseOperator<IntermediateResultsBlock> selectionOperator2 = getOperatorForSegment(SELECTION_QUERY,
        _indexSegments.get(1));
    IntermediateResultsBlock resultsBlock2 = selectionOperator2.nextBlock();
    List<Object[]> selectionResult2 = (List<Object[]>) resultsBlock2.getSelectionResult();

    assertNotNull(selectionResult1);
    assertNotNull(selectionResult2);
    assertEquals(selectionResult1.size(), selectionResult2.size());
    for (int i = 0; i < selectionResult1.size(); i++) {
      Object[] rowQuery1 = selectionResult1.get(i);
      Object[] rowQuery2 = selectionResult2.get(i);

      assertEquals(rowQuery1.length, rowQuery2.length);
      for (int j = 0; j < rowQuery1.length; j++) {
        assertEquals(rowQuery1[j], rowQuery2[j]);
      }
    }

    // Select columns with filters
    brokerResponseNative = getBrokerResponse(SELECTION_QUERY + FILTER);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 40L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 200L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 192964L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), DATA_SCHEMA_SELECTION);
    resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 5);
      assertEquals((String) resultRow[1], "gFuH");
    }

    firstRow = resultRows.get(0);
    // Column 1
    assertEquals(((Integer) firstRow[0]).intValue(), 351823652);
    // Column 11
    assertEquals((String) firstRow[4], "t");

    // Validate that results returned for each segment (especially for columns which have different dictionary with
    // compression flags) are the same
    selectionOperator1 = getOperatorForSegment(SELECTION_QUERY + FILTER, _indexSegments.get(0));
    resultsBlock1 = selectionOperator1.nextBlock();
    selectionResult1 = (List<Object[]>) resultsBlock1.getSelectionResult();

    selectionOperator2 = getOperatorForSegment(SELECTION_QUERY + FILTER, _indexSegments.get(1));
    resultsBlock2 = selectionOperator2.nextBlock();
    selectionResult2 = (List<Object[]>) resultsBlock2.getSelectionResult();

    assertNotNull(selectionResult1);
    assertNotNull(selectionResult2);
    assertEquals(selectionResult1.size(), selectionResult2.size());
    for (int i = 0; i < selectionResult1.size(); i++) {
      Object[] rowQuery1 = selectionResult1.get(i);
      Object[] rowQuery2 = selectionResult2.get(i);

      assertEquals(rowQuery1.length, rowQuery2.length);
      for (int j = 0; j < rowQuery1.length; j++) {
        assertEquals(rowQuery1[j], rowQuery2[j]);
      }
    }

    String query1 = "SELECT 'marvin' from testTable";
    brokerResponseNative = getBrokerResponse(query1);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 40L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 0L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"'marvin'"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}));
    resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 1);
      assertEquals((String) resultRow[0], "marvin");
    }

    // Select query with a filter on a column without inverted index but with dictionary with compression enabled
    String query2 = "SELECT column1, column5, column9 from testTable WHERE column9 < 50000";
    brokerResponseNative = getBrokerResponse(query2);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 4);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 12L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 120000L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "column5", "column9"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT}));
    resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 3);
    }

    // Select query with a filter on a column with inverted index and with dictionary with compression enabled
    String query3 = "SELECT column1, column5, column6 from testTable WHERE column6 > 50000 AND column6 < 1700000";
    brokerResponseNative = getBrokerResponse(query3);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 4);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 12L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 120000L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "column5", "column6"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT}));
    resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 3);
    }

    // Transform function on a filter clause for a dictionary with compression column without inverted index
    String query4 = "SELECT column1, column6 from testTable WHERE CONCAT(column5, column9, '-') = 'gFuH-5000'";
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query4));

    // Transform function on a filter clause for a dictionary with compression column with inverted index
    String query5 = "SELECT column1, column5 from testTable WHERE CONCAT(column6, column11, '-') = 'gFuH-5000'";
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query5));
  }

  /**
   * Dictionary with compression doesn't work with transforms yet. Ensure that when dictionary with compression is
   * enabled on any columns, the query fails. Transforms should work fine if there are no dictionary with compression
   * columns.
   */
  @Test
  public void testSelectWithTransform() {
    // Select and concat dictionary with compression enabled columns without any filters
    final String query1 = "SELECT CONCAT(column5, column6, ':') from testTable";
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query1));

    // Validate that the results returned by each segment also fails
    assertThrows(BadQueryRequestException.class, () -> getOperatorForSegment(query1, _indexSegments.get(0)));
    assertThrows(BadQueryRequestException.class, () -> getOperatorForSegment(query1, _indexSegments.get(1)));

    final String query2 = "SELECT CONCAT(column9, column11, ':') from testTable";
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query2));

    // Validate that the results returned by the first segment also fails
    assertThrows(BadQueryRequestException.class, () -> getOperatorForSegment(query2, _indexSegments.get(0)));

    // Validate that the results returned by the second segment doesn't fail because dictionary with compression isn't
    // enabled on these columns for this segment
    BaseOperator<IntermediateResultsBlock> selectionOperator2 = getOperatorForSegment(query2,
        _indexSegments.get(1));
    IntermediateResultsBlock resultsBlock2 = selectionOperator2.nextBlock();
    List<Object[]> selectionResult2 = (List<Object[]>) resultsBlock2.getSelectionResult();
    assertNotNull(selectionResult2);
    assertEquals(selectionResult2.size(), 10);

    // Select and concat one non-dictionary with compression enabled column and one with it enabled without any filters
    final String query3 = "SELECT CONCAT(column1, column6, ':') from testTable";
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query3));

    // Validate that the results returned by each segment also fails
    assertThrows(BadQueryRequestException.class, () -> getOperatorForSegment(query3, _indexSegments.get(0)));
    assertThrows(BadQueryRequestException.class, () -> getOperatorForSegment(query3, _indexSegments.get(1)));

    // Select and concat one non-dictionary with compression enabled column and one with it enabled without any filters
    // the dictionary with compression column has some segments with this disabled.
    final String query4 = "SELECT CONCAT(column1, column11, ':') from testTable";
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query4));

    // Validate that the results returned by the first segment also fails
    assertThrows(BadQueryRequestException.class, () -> getOperatorForSegment(query4, _indexSegments.get(0)));

    // Validate that the results returned by the second segment doesn't fail because dictionary with compression isn't
    // enabled on these columns for this segment
    selectionOperator2 = getOperatorForSegment(query4, _indexSegments.get(1));
    resultsBlock2 = selectionOperator2.nextBlock();
    selectionResult2 = (List<Object[]>) resultsBlock2.getSelectionResult();
    assertNotNull(selectionResult2);
    assertEquals(selectionResult2.size(), 10);

    // Select and concat non-dictionary with compression enabled columns without any filters
    final String query5 = "SELECT CONCAT(column1, column3, ':') from testTable";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query5);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 40L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 80L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"concat(column1,column3,':')"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}));
    List<Object[]> resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 1);
      assertTrue(((String) resultRow[0]).contains(":"));
    }

    // Validate that results returned for each segment for the successful query on both segments is the same
    BaseOperator<IntermediateResultsBlock> selectionOperator1 = getOperatorForSegment(query5,
        _indexSegments.get(0));
    IntermediateResultsBlock resultsBlock1 = selectionOperator1.nextBlock();
    List<Object[]> selectionResult1 = (List<Object[]>) resultsBlock1.getSelectionResult();

    selectionOperator2 = getOperatorForSegment(query5, _indexSegments.get(1));
    resultsBlock2 = selectionOperator2.nextBlock();
    selectionResult2 = (List<Object[]>) resultsBlock2.getSelectionResult();

    assertNotNull(selectionResult1);
    assertNotNull(selectionResult2);
    assertEquals(selectionResult1.size(), selectionResult2.size());
    for (int i = 0; i < selectionResult1.size(); i++) {
      Object[] rowQuery1 = selectionResult1.get(i);
      Object[] rowQuery2 = selectionResult2.get(i);

      assertEquals(rowQuery1.length, rowQuery2.length);
      for (int j = 0; j < rowQuery1.length; j++) {
        assertEquals(rowQuery1[j], rowQuery2[j]);
      }
    }

    // Select and concat dictionary with compression enabled columns with filters
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query1 + FILTER));

    // Validate that the results returned by each segment also fails
    assertThrows(BadQueryRequestException.class, () -> getOperatorForSegment(query1 + FILTER, _indexSegments.get(0)));
    assertThrows(BadQueryRequestException.class, () -> getOperatorForSegment(query1 + FILTER, _indexSegments.get(1)));

    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query2 + FILTER));

    // Validate that the results returned by the first segment also fails
    assertThrows(BadQueryRequestException.class, () -> getOperatorForSegment(query2 + FILTER, _indexSegments.get(0)));

    // Validate that the results returned by the second segment doesn't fail because dictionary with compression isn't
    // enabled on these columns for this segment
    selectionOperator2 = getOperatorForSegment(query2 + FILTER, _indexSegments.get(1));
    resultsBlock2 = selectionOperator2.nextBlock();
    selectionResult2 = (List<Object[]>) resultsBlock2.getSelectionResult();
    assertNotNull(selectionResult2);
    assertEquals(selectionResult2.size(), 10);

    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query3 + FILTER));

    // Validate that the results returned by each segment also fails
    assertThrows(BadQueryRequestException.class, () -> getOperatorForSegment(query3 + FILTER, _indexSegments.get(0)));
    assertThrows(BadQueryRequestException.class, () -> getOperatorForSegment(query3 + FILTER, _indexSegments.get(1)));

    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query4 + FILTER));

    // Validate that the results returned by the first segment also fails
    assertThrows(BadQueryRequestException.class, () -> getOperatorForSegment(query4 + FILTER, _indexSegments.get(0)));

    // Validate that the results returned by the second segment doesn't fail because dictionary with compression isn't
    // enabled on these columns for this segment
    selectionOperator2 = getOperatorForSegment(query4 + FILTER, _indexSegments.get(1));
    resultsBlock2 = selectionOperator2.nextBlock();
    selectionResult2 = (List<Object[]>) resultsBlock2.getSelectionResult();
    assertNotNull(selectionResult2);
    assertEquals(selectionResult2.size(), 10);

    brokerResponseNative = getBrokerResponse(query5 + FILTER);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 40L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 80L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 192964L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"concat(column1,column3,':')"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}));
    resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 1);
      assertTrue(((String) resultRow[0]).contains(":"));
    }

    // Validate that results returned for each segment for the successful query on both segments is the same
    selectionOperator1 = getOperatorForSegment(query5 + FILTER, _indexSegments.get(0));
    resultsBlock1 = selectionOperator1.nextBlock();
    selectionResult1 = (List<Object[]>) resultsBlock1.getSelectionResult();

    selectionOperator2 = getOperatorForSegment(query5 + FILTER, _indexSegments.get(1));
    resultsBlock2 = selectionOperator2.nextBlock();
    selectionResult2 = (List<Object[]>) resultsBlock2.getSelectionResult();

    assertNotNull(selectionResult1);
    assertNotNull(selectionResult2);
    assertEquals(selectionResult1.size(), selectionResult2.size());
    for (int i = 0; i < selectionResult1.size(); i++) {
      Object[] rowQuery1 = selectionResult1.get(i);
      Object[] rowQuery2 = selectionResult2.get(i);

      assertEquals(rowQuery1.length, rowQuery2.length);
      for (int j = 0; j < rowQuery1.length; j++) {
        assertEquals(rowQuery1[j], rowQuery2[j]);
      }
    }
  }

  /**
   * Distinct operator decides the DistinctExecutor to use based on whether the transform has a dictionary or not.
   * Due to this without adding support to transform for the dictionary with compression flag, DISTINCT queries
   * cannot be supported.
   */
  @Test
  public void testSelectWithDistinct() {
    // Select a mix of dictionary with compression and non-dictionary with compression columns with distinct
    String query = "SELECT DISTINCT column1, column5, column6, column9, column11 FROM testTable LIMIT 100";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
    List<QueryProcessingException> processingExceptions = brokerResponseNative.getProcessingExceptions();
    assertNotNull(processingExceptions);
    assertEquals(processingExceptions.size(), 2);
    for (QueryProcessingException processingException : processingExceptions) {
      assertTrue(processingException.getMessage().contains("java.lang.UnsupportedOperationException"));
    }

    // Running DISTINCT on a column which has dictionary with compression enabled on one segment fails
    final String query1 = "SELECT DISTINCT column9, column11 FROM testTable LIMIT 100";
    BaseOperator<IntermediateResultsBlock> distinctOperator1 = getOperatorForSegment(query1,
        _indexSegments.get(0));
    assertThrows(UnsupportedOperationException.class, distinctOperator1::nextBlock);

    // Running DISTINCT on a column which has dictionary with compression disabled on one segment works
    BaseOperator<IntermediateResultsBlock> distinctOperator2 = getOperatorForSegment(query1,
        _indexSegments.get(1));
    IntermediateResultsBlock resultsBlock2 = distinctOperator2.nextBlock();
    List<Object> aggregationResult2 = resultsBlock2.getAggregationResult();
    assertNotNull(aggregationResult2);
    assertEquals(aggregationResult2.size(), 1);
    assertEquals(((DistinctTable) aggregationResult2.get(0)).getRecords().size(), 100);

    // Broker response indicates error as one segment in each server has the segment with the columns with dictionary
    // with compression enabled.
    brokerResponseNative = getBrokerResponse(query1);
    processingExceptions = brokerResponseNative.getProcessingExceptions();
    assertNotNull(processingExceptions);
    assertEquals(processingExceptions.size(), 2);
    for (QueryProcessingException processingException : processingExceptions) {
      assertTrue(processingException.getMessage().contains("java.lang.UnsupportedOperationException"));
    }

    // Select non-dictionary with compression columns with distinct
    query = "SELECT DISTINCT column1, column3 FROM testTable LIMIT 100";
    brokerResponseNative = getBrokerResponse(query);
    processingExceptions = brokerResponseNative.getProcessingExceptions();
    assertNotNull(processingExceptions);
    assertEquals(processingExceptions.size(), 0);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 100);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 40000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 80000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "column3"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT}));
    List<Object[]> resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 2);
    }
  }

  /**
   * GroupBy OrderBy operator decides the Key Executor to use based on whether the transform has a dictionary or not.
   * Due to this without adding support to transform for the dictionary with compression flag, some GROUP BY or GROUP
   * BY ORDER BY queries cannot be supported.
   */
  @Test
  public void testSelectWithAggregate() {
    // Aggregation on dictionary with compression columns
    final String query1 = "SELECT max(column6), min(column9) from testTable";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query1);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 0L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(column6)", "min(column9)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE}));
    List<Object[]> resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 2);
      assertEquals(resultRow[0], 2.147458029E9);
      assertEquals(resultRow[1], 11270.0);
    }

    // Aggregation on a mix of dictionary with compression and non-dictionary with compression columns
    final String query2 = "SELECT max(column6), sum(column9), min(column7) from testTable";
    brokerResponseNative = getBrokerResponse(query2);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 360000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(column6)", "sum(column9)",
        "min(column7)"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE,
        DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE}));
    resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 3);
      assertEquals(resultRow[0], 2.147458029E9);
      assertEquals(resultRow[1], 8.2586661013328E13);
      assertEquals(resultRow[2], 675695.0);
    }

    // Aggregation on dictionary with compression columns and group-by on non-dictionary with compression column
    final String query3 = "SELECT max(column6), min(column9), column17 from testTable GROUP BY column17 LIMIT 100";
    brokerResponseNative = getBrokerResponse(query3);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 24);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 360000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(column6)", "min(column9)", "column17"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
        DataSchema.ColumnDataType.INT}));
    resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 3);
    }

    // Aggregation on dictionary with compression columns and group-by on dictionary with compression column
    // This will have a query processing exception because the group-by column is dictionary encoded but has raw data
    final String query4 = "SELECT max(column6), min(column9), column11 from testTable GROUP BY column11 LIMIT 100";
    brokerResponseNative = getBrokerResponse(query4);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 5);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 80_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 240000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(column6)", "min(column9)", "column11"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
            DataSchema.ColumnDataType.STRING}));
    resultRows = resultTable.getRows();
    Set<String> column11StringSet = new HashSet<>(Arrays.asList("t", "gFuH", "o", "", "P"));
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 3);
      assertTrue(column11StringSet.contains((String) resultRow[2]));
    }
    List<QueryProcessingException> processingExceptions = brokerResponseNative.getProcessingExceptions();
    assertNotNull(processingExceptions);
    assertEquals(processingExceptions.size(), 2);
    for (QueryProcessingException processingException : processingExceptions) {
      assertTrue(processingException.getMessage().contains("java.lang.UnsupportedOperationException"));
    }

    // Aggregation on dictionary with compression columns and group-by order-by on non-dictionary with compression
    // column
    final String query5 = "SELECT max(column6), min(column9), column17 from testTable GROUP BY column17 ORDER BY "
        + "column17 LIMIT 100";
    brokerResponseNative = getBrokerResponse(query5);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 24);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 360000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(column6)", "min(column9)", "column17"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
            DataSchema.ColumnDataType.INT}));
    resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 3);
    }

    // Aggregation on dictionary with compression columns and group-by order-by on dictionary with compression column
    // This will have a query processing exception because the group-by column is dictionary encoded but has raw data
    final String query6 = "SELECT max(column6), min(column9), column11 from testTable GROUP BY column11 ORDER BY "
        + "column11 LIMIT 100";
    brokerResponseNative = getBrokerResponse(query6);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 5);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 80_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 240000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(column6)", "min(column9)", "column11"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
            DataSchema.ColumnDataType.STRING}));
    resultRows = resultTable.getRows();
    List<String> column11StringList = new ArrayList<>(Arrays.asList("", "P", "gFuH", "o", "t"));
    int index = 0;
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 3);
      assertEquals(column11StringList.get(index++), resultRow[2]);
    }
    processingExceptions = brokerResponseNative.getProcessingExceptions();
    assertNotNull(processingExceptions);
    assertEquals(processingExceptions.size(), 2);
    for (QueryProcessingException processingException : processingExceptions) {
      assertTrue(processingException.getMessage().contains("java.lang.UnsupportedOperationException"));
    }

    // Aggregation on dictionary with compression columns and group-by order-by on non-dictionary with compression
    // column
    final String query7 = "SELECT max(column6), min(column9), column17 from testTable GROUP BY column17 ORDER BY "
        + "column17, max(column1) DESC LIMIT 100";
    brokerResponseNative = getBrokerResponse(query7);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 24);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 480000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(column6)", "min(column9)", "column17"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
            DataSchema.ColumnDataType.INT}));
    resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 3);
    }

    // Aggregation on dictionary with compression columns and group-by order-by on dictionary with compression column
    // This will have a query processing exception because the group-by column is dictionary encoded but has raw data
    final String query8 = "SELECT max(column6), min(column9), column11 from testTable GROUP BY column11 ORDER BY "
        + "column11, max(column9) DESC LIMIT 100";
    brokerResponseNative = getBrokerResponse(query8);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 5);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 80_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 240000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(column6)", "min(column9)", "column11"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
            DataSchema.ColumnDataType.STRING}));
    resultRows = resultTable.getRows();
    column11StringList = new ArrayList<>(Arrays.asList("", "P", "gFuH", "o", "t"));
    index = 0;
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 3);
      assertEquals(column11StringList.get(index++), resultRow[2]);
    }
    processingExceptions = brokerResponseNative.getProcessingExceptions();
    assertNotNull(processingExceptions);
    assertEquals(processingExceptions.size(), 2);
    for (QueryProcessingException processingException : processingExceptions) {
      assertTrue(processingException.getMessage().contains("java.lang.UnsupportedOperationException"));
    }

    // Aggregation on dictionary with compression columns with one transform on dictionary with compression column and
    // group-by on transform on dict with compression
    final String query9 = "SELECT max(column6), concat(column9, column17, '-') from testTable GROUP BY "
        + "concat(column9, column17, '-') LIMIT 100";
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query9));

    // Aggregation on dictionary with compression columns with one transform on dictionary with compression column and
    // group-by on transform on dict with compression, order-by some column
    final String query10 = "SELECT max(column6), concat(column9, column17, '-') from testTable GROUP BY "
        + "concat(column9, column17, '-') ORDER BY concat(column9, column17, '-') LIMIT 100";
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query10));

    // Aggregation on dictionary with compression columns with one transform on non-dictionary with compression column
    // and group-by on transform on non-dict with compression
    // This throws in-spite of the fact that only column6 is a dictionary with compression column, and the actual
    // transform columns aren't dictionary with compression columns
    final String query11 = "SELECT max(column6), concat(column1, column17, '-') from testTable GROUP BY "
        + "concat(column1, column17, '-') LIMIT 100";
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query11));

    // Aggregation on dictionary with compression columns with one transform on dictionary with compression column and
    // group-by on transform on dict with compression, order-by some column
    // This throws in-spite of the fact that only column6 is a dictionary with compression column, and the actual
    // transform columns aren't dictionary with compression columns
    final String query12 = "SELECT max(column6), concat(column1, column17, '-') from testTable GROUP BY "
        + "concat(column1, column17, '-') ORDER BY concat(column1, column17, '-') LIMIT 100";
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query12));

    // Transform inside aggregation with a column with dictionary with compression and one without
    final String query13 = "SELECT SUM(ADD(column7, column9)) from testTable LIMIT 100";
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query13));

    // Transform inside aggregation with a column without dictionary with compression
    final String query14 = "SELECT SUM(ADD(column7, column7)) from testTable LIMIT 100";
    brokerResponseNative = getBrokerResponse(query14);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 120000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"sum(add(column7,column7))"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE}));
    resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 1);
    }

    // Transform inside aggregation with a column with dictionary with compression and one without with group-by
    final String query15 = "SELECT column1, SUM(ADD(column7, column9)) from testTable GROUP BY column1 LIMIT 100";
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query15));

    // Transform inside aggregation with a column without dictionary with compression with group-by
    final String query16 = "SELECT column1, SUM(ADD(column7, column7)) from testTable GROUP BY column1 LIMIT 100";
    brokerResponseNative = getBrokerResponse(query16);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 100);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 240000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "sum(add(column7,column7))"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE}));
    resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 2);
    }

    // Transform inside aggregation with a column with dictionary with compression and one without with group-by
    // and order-by
    final String query17 = "SELECT column1, SUM(ADD(column7, column9)) from testTable GROUP BY column1 ORDER BY "
        + "column1 LIMIT 100";
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query17));

    // Transform inside aggregation with a column without dictionary with compression with group-by and order-by
    final String query18 = "SELECT column1, SUM(ADD(column7, column7)) from testTable GROUP BY column1 ORDER BY "
        + "column1 LIMIT 100";
    brokerResponseNative = getBrokerResponse(query18);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 100);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 240000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "sum(add(column7,column7))"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE}));
    resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 2);
    }
  }

  @Test
  public void testSelectWithAggregateWithFilters() {
    // Aggregation query with a filter on a column without inverted index but with dictionary with compression enabled
    String query1 = "SELECT column1, sum(column9) from testTable WHERE column9 < 50000 GROUP BY column1 LIMIT 10";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query1);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 8L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 120000L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "sum(column9)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE}));
    List<Object[]> resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 2);
    }

    // Aggregation query with a filter on a column with inverted index and with dictionary with compression enabled
    String query2 = "SELECT column1, sum(column6) from testTable WHERE column6 > 50000 AND column6 < 1700000"
        + "GROUP BY column1 LIMIT 10";
    brokerResponseNative = getBrokerResponse(query2);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 8L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 120000L);
    assertNotNull(brokerResponseNative.getProcessingExceptions());
    assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "sum(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE}));
    resultRows = resultTable.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 2);
    }
  }

  /**
   * Run query on single index segment provided as input.
   * <p>Use this to test a single operator.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private <T extends Operator> T getOperatorForSegment(String query, IndexSegment segment) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    PinotQuery serverPinotQuery = GapfillUtils.stripGapfill(pinotQuery);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(serverPinotQuery);
    return (T) PLAN_MAKER.makeSegmentPlanNode(segment, queryContext).run();
  }
}
