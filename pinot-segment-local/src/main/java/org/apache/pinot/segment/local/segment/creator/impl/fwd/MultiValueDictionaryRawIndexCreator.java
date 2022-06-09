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
import java.nio.ByteBuffer;
import org.apache.pinot.segment.local.io.writer.impl.BaseChunkSVForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteDictionaryChunkSVForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteDictionaryChunkSVForwardIndexWriterV4;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Forward index creator for raw dictionary multi-value column of fixed length data type (INT as dictionary IDs are int)
 */
public class MultiValueDictionaryRawIndexCreator implements ForwardIndexCreator {

  private static final int DEFAULT_NUM_DOCS_PER_CHUNK = 1000;
  private static final int TARGET_MAX_CHUNK_SIZE = 1024 * 1024;

  private final VarByteChunkWriter _indexWriter;
  private final FieldSpec.DataType _valueType;

  /**
   * Create a var-byte raw index creator for the given column
   *
   * @param baseIndexDir Index directory
   * @param compressionType Type of compression to use
   * @param column Name of column to index
   * @param totalDocs Total number of documents to index
   */
  public MultiValueDictionaryRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs, int maxNumberOfMultiValueElements)
      throws IOException {
    this(baseIndexDir, compressionType, column, totalDocs, maxNumberOfMultiValueElements, false,
        BaseChunkSVForwardIndexWriter.DEFAULT_VERSION);
  }

  /**
   * Create a var-byte raw index creator for the given column
   *
   * @param baseIndexDir Index directory
   * @param compressionType Type of compression to use
   * @param column Name of column to index
   * @param totalDocs Total number of documents to index
   * @param deriveNumDocsPerChunk true if writer should auto-derive the number of rows per chunk
   * @param writerVersion writer format version
   */
  public MultiValueDictionaryRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType,
      String column, int totalDocs, int maxNumberOfMultiValueElements, boolean deriveNumDocsPerChunk, int writerVersion)
      throws IOException {
    File file = new File(baseIndexDir, column + V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION);
    int totalMaxLength = maxNumberOfMultiValueElements * FieldSpec.DataType.INT.getStoredType().size();
    int numDocsPerChunk =
        deriveNumDocsPerChunk ? Math.max(TARGET_MAX_CHUNK_SIZE / (totalMaxLength
            + VarByteDictionaryChunkSVForwardIndexWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE), 1)
            : DEFAULT_NUM_DOCS_PER_CHUNK;
    _indexWriter = writerVersion < VarByteDictionaryChunkSVForwardIndexWriterV4.VERSION
        ? new VarByteDictionaryChunkSVForwardIndexWriter(file, compressionType, totalDocs, numDocsPerChunk,
        totalMaxLength, writerVersion)
        : new VarByteDictionaryChunkSVForwardIndexWriterV4(file, compressionType, TARGET_MAX_CHUNK_SIZE);
    _valueType = FieldSpec.DataType.INT;
  }

  @Override
  public boolean isDictionaryEncoded() {
    return true;
  }

  @Override
  public boolean isSingleValue() {
    return false;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _valueType;
  }

  @Override
  public void putDictIdMV(int[] dictIds) {
    byte[] bytes = new byte[Integer.BYTES
        + dictIds.length * Integer.BYTES]; //numValues, bytes required to store the content
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    //write the length
    byteBuffer.putInt(dictIds.length);
    //write the content of each element
    for (final int dictId : dictIds) {
      byteBuffer.putInt(dictId);
    }
    _indexWriter.putBytes(bytes);
  }

  @Override
  public void close()
      throws IOException {
    _indexWriter.close();
  }
}
