/**
 * Copyright 2022 Dremio
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.support.diagnostics.repro.parse;

import com.dremio.support.diagnostics.repro.PDSDataProvider;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

public class JsonFormatter {

  private static final Logger logger = Logger.getLogger(JsonFormatter.class.getName());
  private static final String nullString = null;
  private static final int maxListSize = 10;
  // formatter for timestamps and date values, assuming US and UTC for consistency
  private final DateTimeFormatter dateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
          .withLocale(Locale.US)
          .withZone(ZoneId.of("UTC"));
  // time formatter, assuming US locale and UTC for consistency
  private final DateTimeFormatter timeFormatter =
      DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withLocale(Locale.US).withZone(ZoneId.of("UTC"));

  public String getJsonStringFromField(final Field field, final PDSDataProvider dataProvider) {
    final ArrowType.ArrowTypeID type = field.getType().getTypeID();
    switch (type) {
      case Binary:
      case LargeBinary:
        final Encoder encoder = Base64.getEncoder();
        final String newString = dataProvider.getString();
        final byte[] newStringBytes = newString.getBytes(StandardCharsets.UTF_8);
        final String encodedString = encoder.encodeToString(newStringBytes);
        return String.format("\"%s\"", encodedString);
      case LargeList:
      case FixedSizeList:
      case List:
        {
          StringBuilder builder = new StringBuilder();
          builder.append("[");
          final List<String> elements = new ArrayList<>();
          for (int i = 0; i < Math.min(dataProvider.getInt(), maxListSize); i++) {
            final List<String> generatedDataPerType =
                field.getChildren().stream()
                    .map(x -> getJsonStringFromField(x, dataProvider))
                    .collect(Collectors.toList());
            elements.addAll(generatedDataPerType);
          }
          builder.append(String.join(", ", elements));
          builder.append("]");
          return builder.toString();
        }
      case Struct:
        {
          StringBuilder builder = new StringBuilder();
          final List<String> rawList =
              field.getChildren().stream()
                  .map(
                      x -> {
                        final String data = getJsonStringFromField(x, dataProvider);
                        return String.format("\"%s\": %s", x.getName(), data);
                      })
                  .collect(Collectors.toList());
          builder.append("{");
          builder.append(String.join(", ", rawList));
          builder.append("}");
          return builder.toString();
        }
      case Null:
        return nullString;
      case Bool:
        return String.valueOf(dataProvider.getBoolean());
      case FloatingPoint:
        final FloatingPointPrecision precision =
            ((ArrowType.FloatingPoint) field.getType()).getPrecision();
        switch (precision) {
          case SINGLE:
            return String.format("{ \"$numberDouble\": %.4f}", dataProvider.getFloat());
          case DOUBLE:
            return String.format("{ \"$numberDouble\": %.4f}", dataProvider.getDouble());
          default:
            throw new IllegalStateException("Unsupported precision: " + precision);
        }
      case Int:
        final int bitWidth = ((ArrowType.Int) field.getType()).getBitWidth();
        switch (bitWidth) {
          case 64:
            return String.format("{ \"$numberLong\": %d}", dataProvider.getLong());
          case 32:
            return String.format("{ \"$numberInt\": %d}", dataProvider.getInt());
          default:
            throw new IllegalStateException("Unsupported bitWidth: " + bitWidth);
        }
      case LargeUtf8:
      case Utf8:
        return String.format("\"%s\"", dataProvider.getString());
      case Decimal:
        return String.format("{ \"$numberDecimal\": %.2f}", dataProvider.getDouble());
      case Date:
        return String.format(
            "{ \"$date\": \"%sZ\"}",
            dataProvider
                .getLocalDate()
                .atStartOfDay(ZoneOffset.UTC)
                .format(dateTimeFormatter)
                .replace(" ", "T"));
      case Time:
        return String.format("{ \"$date\": \"%s\"}", dataProvider.getTime().format(timeFormatter));
      case Timestamp:
        final Instant instant = dataProvider.getInstant();
        try {
          return String.format(
              "{ \"$date\": \"%sZ\"}", dateTimeFormatter.format(instant).replace(" ", "T"));
        } catch (final Exception e) {
          logger.severe(() -> String.format("unable to handle instant of %s", instant));
          logger.log(Level.SEVERE, "unhandled exception", e);
          return null;
        }
      default:
        logger.warning(() -> String.format("unsupported type %s returning null", type));
        return null;
    }
  }
}
