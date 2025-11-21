/*
 * Copyright YugabyteDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveLongCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class UInt32Codec implements PrimitiveLongCodec {

  @NonNull
  @Override
  public GenericType<Long> getJavaType() {
    return GenericType.LONG;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.UINT32;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    // UInt32Codec should only match when CQL type is explicitly UINT32.
    // For value-only lookups, we return false so that BigIntCodec (the default for Long)
    // is selected instead. This prevents UInt32Codec from matching Long values
    // when no CQL type is provided, since inferCqlTypeFromValue() returns BIGINT for Long.
    // When codecFor(DataTypes.UINT32, value) is called, the codec is selected by protocol code
    // first, and if accepts() returns false, it falls through to getCachedCodec() which
    // will still work correctly.
    return false;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return javaClass == Long.class || javaClass == long.class;
  }

  @Nullable
  @Override
  public ByteBuffer encodePrimitive(long value, @NonNull ProtocolVersion protocolVersion) {
    if (value < 0 || value > 0xFFFFFFFFL) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid unsigned 32-bits integer value: %d (must be between 0 and %d)",
              value, 0xFFFFFFFFL));
    }
    ByteBuffer bytes = ByteBuffer.allocate(4);
    // Write as unsigned 32-bit integer
    bytes.putInt(0, (int) value);
    return bytes;
  }

  @Override
  public long decodePrimitive(
      @Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return 0;
    } else if (bytes.remaining() != 4) {
      throw new IllegalArgumentException(
          "Invalid unsigned 32-bits integer value, expecting 4 bytes but got " + bytes.remaining());
    } else {
      // Read as unsigned 32-bit integer
      int signed = bytes.getInt(bytes.position());
      // Convert signed int to unsigned long
      return signed & 0xFFFFFFFFL;
    }
  }

  @NonNull
  @Override
  public String format(@Nullable Long value) {
    return (value == null) ? "NULL" : Long.toUnsignedString(value);
  }

  @Nullable
  @Override
  public Long parse(@Nullable String value) {
    try {
      return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
          ? null
          : Long.parseUnsignedLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Cannot parse unsigned 32-bits integer value from \"%s\"", value), e);
    }
  }
}
