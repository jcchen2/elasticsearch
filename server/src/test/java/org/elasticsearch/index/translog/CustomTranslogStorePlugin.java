/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.translog;

import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.mockfile.FilterFileChannel;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

public class CustomTranslogStorePlugin extends Plugin implements IndexStorePlugin {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Collections.emptyMap();
    }

    @Override
    public Function<IndexSettings, ChannelFactory> getChannelFactoryProvider() {
        return indexSettings -> new CustomChannelFactory();
    }

    public static class CustomChannelFactory implements ChannelFactory {
        protected final Logger logger = LogManager.getLogger(getClass());

        @Override
        public FileChannel open(Path path, OpenOption... options) throws IOException {
            logger.debug("open {}", path);
            return new HexFileChannel(FileChannel.open(path, options));
        }
    }

    // a hex-encoded file channel
    public static class HexFileChannel extends FilterFileChannel {
        protected final Logger logger = LogManager.getLogger(getClass());

        public HexFileChannel(FileChannel inner) {
            super(inner);
        }

        @Override
        public long size() throws IOException {
            long size = toDecodedLen(super.size());
            logger.debug("size {}", size);
            return size;
        }

        @Override
        public FileChannel truncate(long size) throws IOException {
            logger.debug("truncate to {}", size);
            super.truncate(toEncodedLen(size));
            return this;
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            logger.debug("set position to {}", newPosition);
            return super.position(toEncodedLen(newPosition));
        }

        @Override
        public long position() throws IOException {
            long pos = toDecodedLen(super.position());
            logger.debug("position {}", pos);
            return pos;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(toEncodedLen(dst.limit()));
            int len = super.read(buf);
            byte[] bytes = decode(buf.array(), 0, len);
            dst.put(bytes);
            int decodedLen = toDecodedLen(len);
            logger.debug("read {} into {}", decodedLen, dst);
            return decodedLen;
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(toEncodedLen(dst.limit()));
            int len = super.read(buf, toEncodedLen(position));
            dst.put(decode(buf.array(), 0, len));
            int decodedLen = toDecodedLen(len);
            logger.debug("read {} at {} into {}", decodedLen, position, dst);
            return decodedLen;
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            long sum = 0;
            for (int i=0; i<length; i++) {
                sum = read(dsts[offset+i]);
            }
            logger.debug("read {} into buffers {}", sum, dsts);
            return sum;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            byte[] bytes = new byte[src.limit()];
            src.get(bytes);
            int len = toDecodedLen(super.write(ByteBuffer.wrap(encode(bytes))));
            logger.debug("write {} bytes from {}", len, src);
            return len;
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            byte[] bytes = new byte[src.limit()];
            src.get(bytes);
            int len = toDecodedLen(super.write(ByteBuffer.wrap(encode(bytes)), toEncodedLen(position)));
            logger.debug("write {} bytes at {} from {}", len, position, src);
            return len;
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            long sum = 0;
            for (int i=0; i<length; i++) {
                sum += write(srcs[offset+i]);
            }
            logger.debug("write {} from buffers {}", sum, srcs);
            return sum;
        }

        // hex string to bytes
        private byte[] decode(byte[] src, int offset, int len) {
            try {
                return Hex.decodeHex(new String(src, offset, len, UTF8));
            } catch (Exception e) {
                throw new RuntimeException("unable to decode", e);
            }
        }

        // bytes to hex string
        private byte[] encode(byte[] bytes) {
            return Hex.encodeHexString(bytes, true).getBytes(UTF8);
        }

        private long toEncodedLen(long len) {
            return len * 2;
        }

        private int toEncodedLen(int len) {
            return len * 2;
        }

        private long toDecodedLen(long len) {
            return len / 2;
        }

        private int toDecodedLen(int len) {
            return len / 2;
        }
    }

}
