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
import com.dremio.support.diagnostics.repro.SchemaDeserializer;
import com.dremio.support.diagnostics.shared.dto.profilejson.DatasetProfile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * ArrowFormatDeserializer uses arrow-format, arrow-vector and flatbuff types to generate a valid
 * schema from the batchSchema of the profile.json
 */
public class ArrowFormatDeserializer implements SchemaDeserializer {

  private static final Logger logger = Logger.getLogger(ArrowFormatDeserializer.class.getName());
  // formatter for date values
  private static final DateTimeFormatter dateFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd").withLocale(Locale.US).withZone(ZoneId.of("UTC"));
  // formatter for timestamps and date values, assuming US and UTC for consistency
  private static final DateTimeFormatter dateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
          .withLocale(Locale.US)
          .withZone(ZoneId.of("UTC"));
  // time formatter, assuming US locale and UTC for consistency
  private static final DateTimeFormatter timeFormatter =
      DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withLocale(Locale.US).withZone(ZoneId.of("UTC"));
  private JsonFormatter jsonFormatter = new JsonFormatter();
  // number or records to generate for each file
  private final long records;
  // provides random values
  private final PDSDataProvider random;
  // provides known values
  private final PDSDataProvider constant;
  // overrides for column datas
  private final ColumnDefYaml columnDef;

  /**
   * @param records the number the records to generate per file
   */
  public ArrowFormatDeserializer(final long records, final ColumnDefYaml columnDef) {
    this(records, new PDSConstantDataProvider(), new PDSRandomDataProvider(), columnDef);
  }

  /**
   * @param records the number the records to generate per file
   * @param constant the data provider strategy for the "constant" provider, this is used on the
   *     first row
   * @param random the data provider strategy for the random, this is used on most rows
   */
  public ArrowFormatDeserializer(
      final long records,
      final PDSDataProvider constant,
      final PDSDataProvider random,
      final ColumnDefYaml columnDef) {
    this.records = records;
    this.constant = constant;
    this.random = random;
    this.columnDef = columnDef;
  }

  /**
   * Support varies according to the jar version specified in pom.xml at the root of the
   * project As of 10/24/2020 this means anything in Arrow Format 1.0 ie 9.0 to 0.3.0 prior. Newer
   * versions of Apache Arrow maybe unsupported and the type fidelity may not be totally accurate
   * even if the class successfully returns text it may vary from what is on the customer system.
   *
   * @see <a href="https://arrow.apache.org/docs/format/Versioning.html">Apache Arrow Versioning</a>
   * @see <a href=
   *     "https://github.com/dremio/dremio/blob/fbced35431e143f8f9652d9a1b6a72e88173db97/oss/pom.xml#L40">Dremio
   *     Arrow Version</a>
   * @param dp dataset profile to convert into a PDS, this needs to be a type 1, or it will fail
   * @return returns clear text schema that has been converted from the arrow format into a pure SQL
   *     method ready to be processed by dremio
   */
  @Override
  public String readSchema(final DatasetProfile dp) {
    final Base64.Decoder d = Base64.getDecoder();
    // assuming UTF-8
    final byte[] schemaBytes = dp.getBatchSchema().getBytes(StandardCharsets.UTF_8);
    // assume it is base64 and decode it
    final byte[] base64Decoded = d.decode(schemaBytes);
    // drop it in a byte buffer and get schema object
    final Schema schema = Schema.getRootAsSchema(ByteBuffer.wrap(base64Decoded));
    // conver to a vector schema, I am not sure why we do this, but we do it in the
    // reproduction
    // tool as it was
    final org.apache.arrow.vector.types.pojo.Schema s =
        org.apache.arrow.vector.types.pojo.Schema.convertSchema(schema);
    // the string that makes the the output file
    final StringBuilder builder = new StringBuilder();

    // remove the $_dremio_$_update_$ field
    // use this as a base for all other field operations
    final List<Field> fields =
        s.getFields().stream()
            .filter(x -> !"$_dremio_$_update_$".equals(x.getName()))
            .collect(Collectors.toList());
    // quote the field names
    final String[] fieldNames =
        fields.stream().map(x -> String.format("\"%s\"", x.getName())).toArray(String[]::new);
    // comma separate them
    final String fieldNameString = String.join(",", fieldNames);
    // create table statement to append to return string
    final String createTableStatement =
        String.format(
            "CREATE TABLE %s as %nSELECT %s %nFROM (values ", dp.getDatasetPath(), fieldNameString);
    // append statement
    builder.append(createTableStatement);
    final String table = dp.getDatasetPath();
    final boolean hasTable;
    if (columnDef != null && columnDef.getTables() != null) {
      hasTable = columnDef.getTables().stream().anyMatch(x -> x.getName().equalsIgnoreCase(table));
    } else {
      hasTable = false;
    }
    final List<ColumnDef> overrides;
    if (!hasTable) {
      overrides = new ArrayList<>();
    } else {
      final TableDef tableDef =
          columnDef.getTables().stream()
              .filter(x -> x.getName().equalsIgnoreCase(table))
              .findFirst()
              .orElse(new TableDef());
      overrides = tableDef.getColumns();
    }
    final List<String> rows = new ArrayList<>();
    final Set<String> columnsOverriden = new HashSet<>();
    for (int i = 0; i < this.records; i++) {
      final List<String> fieldsForRow = new ArrayList<>();
      for (final Field f : fields) {
        if (overrides.stream().anyMatch(x -> x.getName().equalsIgnoreCase(f.getName()))) {
          final ArrayList<String> possibleValues = new ArrayList<>();
          for (final ColumnDef c : overrides) {
            if (c.getName().equalsIgnoreCase(f.getName())) {
              columnsOverriden.add(c.getName());
              for (final String v : c.getValues()) {
                possibleValues.add(v);
              }
            }
          }
          fieldsForRow.add(
              fieldToData(f, new PDSOverrideDataProvider(possibleValues.toArray(new String[0]))));
        } else {
          if (i > 0) {
            // all rows after first use random data
            fieldsForRow.add(fieldToData(f, random));
          } else {
            // we want to throw one predictable row in there for legacy reasons
            fieldsForRow.add(fieldToData(f, constant));
          }
        }
      }
      // wrap in parens
      rows.add(String.format("(%s)", String.join(",", fieldsForRow)));
    }
    if (columnsOverriden.size() != overrides.size()) {
      final List<String> missingColumns =
          overrides.stream()
              .map(x -> x.getName())
              .filter(x -> !columnsOverriden.contains(x))
              .collect(Collectors.toList());
      throw new InvalidColumnOverrideException(
          table,
          missingColumns,
          columnsOverriden.stream().collect(Collectors.toList()),
          fields.stream().map(x -> x.getName()).collect(Collectors.toList()));
    }
    // for readability reasons add a new line and comma separate all the rows
    builder.append(String.join(",\n", rows));
    // finally, close the parens and set the fields inside of an alias using the t()
    // function that
    // makes this all work as a table
    builder.append(String.format(") as t(%s);", fieldNameString));
    return builder.toString();
  }

  /**
   * convert the filed to a valid data string
   *
   * @param f the field to convert
   * @param dataProvider the interface that provides data for the field
   * @return for support data types we get a parens, the data from the provider and any syntax to
   *     make it a valid SQL statement. Unsupported times return empty strings
   */
  public String fieldToData(final Field f, final PDSDataProvider dataProvider) {
    final StringBuilder builder = new StringBuilder();
    final ArrowType.ArrowTypeID type = f.getType().getTypeID();
    switch (type) {
      case Binary:
      case LargeBinary:
      case FixedSizeBinary:
        if (dataProvider.getString() != null) {
          builder.append("BINARY_STRING('");
          builder.append(dataProvider.getString());
          builder.append("')");
        } else {
          builder.append("null");
        }
        break;
      case Duration:
        logger.warning("Duration type detected and not currently supported");
        break;
      case Interval:
        builder.append(dataProvider.getInterval());
        break;
      case FixedSizeList:
      case LargeList:
      case List:
        {
          String data = jsonFormatter.getJsonStringFromField(f, dataProvider);
          builder.append("CONVERT_FROM('");
          builder.append(data);
          builder.append("', 'json')");
        }
        break;
      case Map:
        logger.warning("Map type detected and not currently supported");
        break;
      case NONE:
        logger.warning("NONE type detected and not able to support this");
        break;
      case Null:
        String nullString = null;
        builder.append(nullString);
        break;
      case Union:
        logger.warning(
            "UNION type detected and not able to handle UNION types at this time. Skipping this and"
                + " continuing with the PDS");
        break;
      case Bool:
        builder.append(dataProvider.getBoolean());
        break;
      case FloatingPoint:
        final FloatingPointPrecision precision =
            ((ArrowType.FloatingPoint) f.getType()).getPrecision();
        switch (precision) {
          case SINGLE:
            builder.append("cast(");
            builder.append(dataProvider.getFloat());
            builder.append(" as FLOAT)");
            break;
          case DOUBLE:
            builder.append("cast(");
            builder.append(dataProvider.getDouble());
            builder.append(" as DOUBLE)");
            break;
          default:
            throw new IllegalStateException("Unsupported precision: " + precision);
        }
        break;
      case Int:
        final int bitWidth = ((ArrowType.Int) f.getType()).getBitWidth();
        switch (bitWidth) {
          case 64:
            builder.append("cast(");
            // builder.append(rand.nextLong());
            builder.append(dataProvider.getLong());
            builder.append(" as BIGINT)");
            break;
          case 32:
            builder.append("cast(");
            builder.append(dataProvider.getInt());
            builder.append(" as INTEGER)");
            break;
          default:
            throw new IllegalStateException("Unsupported bitWith: " + bitWidth);
        }
        break;
      case LargeUtf8:
      case Utf8:
        if (dataProvider.getString() != null) {
          builder.append("'");
          builder.append(dataProvider.getString());
          builder.append("'");
        } else {
          builder.append("cast(null as VARCHAR(65536))");
        }
        break;
      case Decimal:
        final ArrowType.Decimal decimalType = ((ArrowType.Decimal) f.getType());
        builder.append("cast(");
        // builder.append(rand.nextDouble());
        builder.append(dataProvider.getDouble());
        builder.append(" as DECIMAL(");
        builder.append(decimalType.getPrecision());
        builder.append(",");
        builder.append(decimalType.getScale());
        builder.append("))");
        break;
      case Date:
        builder.append("cast('");
        builder.append(dataProvider.getLocalDate().format(dateFormatter));
        builder.append("' as DATE)");
        break;
      case Struct:
        {
          String data = jsonFormatter.getJsonStringFromField(f, dataProvider);
          builder.append("CONVERT_FROM('");
          builder.append(data);
          builder.append("', 'json')");
        }
        break;
      case Time:
        builder.append("cast('");
        builder.append(dataProvider.getTime().format(timeFormatter));
        builder.append("' as TIME)");
        break;
      case Timestamp:
        builder.append("cast('");
        final Instant instant = dataProvider.getInstant();
        try {
          builder.append(dateTimeFormatter.format(instant));
        } catch (final Exception e) {
          logger.severe(() -> String.format("unable to handle instant of %s", instant));
          logger.log(Level.SEVERE, "unhandled exception", e);
        }
        builder.append("' AS TIMESTAMP)");
        break;
      default:
        logger.warning(() -> String.format("%s type detected and not currently supported", type));
    }
    return builder.toString();
  }
}
