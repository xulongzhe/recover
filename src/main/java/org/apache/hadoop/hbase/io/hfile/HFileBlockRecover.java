/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import static org.apache.hadoop.hbase.io.compress.Compression.Algorithm.NONE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;

public class HFileBlockRecover {

  Configuration conf = new Configuration();

  boolean includesMemstoreTS;
  boolean includesTag;
  boolean pread;

  private FileSystem fs;

  public static void main(String[] args) throws Exception {

    HFileBlockRecover b = new HFileBlockRecover();
    Path p = new Path("block_test");
    b.genBlock(p);
    b.readBlock(p);
  }

  HFileBlockRecover() throws Exception {
    fs = HFileSystem.get(conf);
    // if (includesTag) {
    // conf.setInt("hfile.format.version", 3);
    // }
  }

  void genBlock(Path path) throws Exception {
    FSDataOutputStream os = fs.create(path);
    HFileContext meta = new HFileContextBuilder()
        .withCompression(NONE)
        .withIncludesMvcc(includesMemstoreTS)
        .withIncludesTags(includesTag)
        .withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM)
        .build();
    HFileBlock.Writer hbw = new HFileBlock.Writer(null,
        meta);
    for (int blockId = 0; blockId < 2; ++blockId) {
      hbw.startWriting(BlockType.DATA);
      for (int i = 0; i < 10; ++i) {
        KeyValue kv = new KeyValue("id".getBytes(), "f".getBytes(), "value".getBytes(), "sdfsdfsdf".getBytes());
        kv.setTimestamp(System.currentTimeMillis());
        hbw.write(kv);
      }
      hbw.writeHeaderAndData(os);
    }
    os.close();
  }

  protected void readBlock(Path path)
      throws Exception {
    FSDataInputStream is = fs.open(path);
    HFileContext meta = new HFileContextBuilder()
        .withHBaseCheckSum(true)
        .withIncludesMvcc(includesMemstoreTS)
        .withIncludesTags(includesTag)
        .withCompression(NONE).build();
    HFileBlock.FSReader hbr = new HFileBlock.FSReaderImpl(is, -1, meta);
    HFileBlock b = hbr.readBlockData(0, -1, pread, false);
    byte[] buf = new byte[1000];
    b.getByteStream().read(buf);
    System.out.println(new String(buf));
    is.close();
  }
}
