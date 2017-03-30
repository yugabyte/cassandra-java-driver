/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;


class LZ4ChecksumCompressor extends ChecksumCompressor {

    static final LZ4ChecksumCompressor INSTANCE = new LZ4ChecksumCompressor();

    final net.jpountz.lz4.LZ4Compressor compressor;
    final net.jpountz.lz4.LZ4FastDecompressor decompressor;

    private LZ4ChecksumCompressor() {
        compressor = LZ4Compressor.INSTANCE.compressor;
        decompressor = LZ4Compressor.INSTANCE.decompressor;
    }

    @Override
    int maxCompressedLength(int length) {
        return compressor.maxCompressedLength(length);
    }

    @Override
    int compressChunk(byte[] src, int length, byte[] dest) {
        return compressor.compress(src, 0, length, dest, 0);
    }

    @Override
    byte[] decompressChunk(byte[] src, int expectedDecompressedLength) {
        return decompressor.decompress(src, expectedDecompressedLength);
    }

}
