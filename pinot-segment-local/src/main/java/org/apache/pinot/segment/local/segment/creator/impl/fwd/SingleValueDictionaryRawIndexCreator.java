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
package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import java.io.File;
import java.io.IOException;
import org.apache.pinot.segment.local.io.writer.impl.BaseChunkSVForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteDictionaryChunkSVForwardIndexWriter;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Forward index creator for raw dictionary single-value column of fixed length data type (INT - as dictionary IDs are
 * integers).
 */
public class SingleValueDictionaryRawIndexCreator implements ForwardIndexCreator {
  private static final int NUM_DOCS_PER_CHUNK = 1000; // TODO: Auto-derive this based on metadata.

  private final FixedByteDictionaryChunkSVForwardIndexWriter _indexWriter;
  private final FieldSpec.DataType _valueType;

  /**
   * Constructor for the class
   *
   * @param baseIndexDir Index directory
   * @param compressionType Type of compression to use
   * @param column Name of column to index
   * @param totalDocs Total number of documents to index
   * @throws IOException
   */
  public SingleValueDictionaryRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs)
      throws IOException {
    this(baseIndexDir, compressionType, column, totalDocs, BaseChunkSVForwardIndexWriter.DEFAULT_VERSION);
  }

  /**
   * Constructor for the class
   *
   * @param baseIndexDir Index directory
   * @param compressionType Type of compression to use
   * @param column Name of column to index
   * @param totalDocs Total number of documents to index
   * @param writerVersion writer format version
   * @throws IOException
   */
  public SingleValueDictionaryRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs, int writerVersion)
      throws IOException {
    File file = new File(baseIndexDir, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    _indexWriter =
        new FixedByteDictionaryChunkSVForwardIndexWriter(file, compressionType, totalDocs, NUM_DOCS_PER_CHUNK,
            FieldSpec.DataType.INT.size(), writerVersion);
    _valueType = FieldSpec.DataType.INT;
  }

  @Override
  public boolean isDictionaryEncoded() {
    return true;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _valueType;
  }

  @Override
  public void putDictId(int dictId) {
    _indexWriter.putDictId(dictId);
  }

  @Override
  public void close()
      throws IOException {
    _indexWriter.close();
  }
}
