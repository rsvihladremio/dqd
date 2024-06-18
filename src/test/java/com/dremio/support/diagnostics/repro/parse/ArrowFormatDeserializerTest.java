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

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.support.diagnostics.repro.PDSDataProvider;
import com.dremio.support.diagnostics.shared.dto.profilejson.DatasetProfile;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

class ArrowFormatDeserializerTest {

  private final String tester = "tester";
  private final PDSConstantDataProvider dp = new PDSConstantDataProvider();
  private final ArrowFormatDeserializer serde =
      new ArrowFormatDeserializer(2L, dp, dp, new ColumnDefYaml());
  private final EmptyDP emptyDP = new EmptyDP();

  public static class EmptyDP implements PDSDataProvider {

    @Override
    public long getLong() {
      return 0;
    }

    @Override
    public float getFloat() {
      return 0;
    }

    @Override
    public double getDouble() {
      return 0;
    }

    @Override
    public int getInt() {
      return 0;
    }

    @Override
    public String getString() {
      return null;
    }

    @Override
    public LocalDate getLocalDate() {
      return null;
    }

    @Override
    public Instant getInstant() {
      return null;
    }

    @Override
    public LocalTime getTime() {
      return null;
    }

    @Override
    public boolean getBoolean() {
      return false;
    }

    @Override
    public List<String> getList() {
      return null;
    }

    @Override
    public String getInterval() {
      return null;
    }
  }

  @Test
  void testDeserializeType() {
    DatasetProfile dp2 = new DatasetProfile();
    dp2.setType(1);
    dp2.setDatasetPath("test.my.path");
    dp2.setBatchSchema(
        "EAAAAAAACgAMAAAACAAEAAoAAAAIAAAACAAAAAAAAAANAAAArAIAAGgCAAAwAgAA+AEAAMABAACMAQAAWAEAACQBAADwAAAAvAAAAIgAAABUAAAABAAAAJr9//8UAAAAFAAAABwAAAAAAAIBIAAAAAAAAAAAAAAACAAMAAgABwAIAAAAAAAAAUAAAAATAAAAJF9kcmVtaW9fJF91cGRhdGVfJADm/f//FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAANT9//8EAAAAVE1JTgAAAAAW/v//FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAAT+//8EAAAAVE1BWAAAAABG/v//FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAADT+//8EAAAAVEFWRwAAAAB2/v//FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAGT+//8EAAAAU05XRAAAAACm/v//FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAJT+//8EAAAAU05PVwAAAADW/v//FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAMT+//8EAAAAUFJDUAAAAAAG////FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAPT+//8EAAAAREFURQAAAAA2////FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAACT///8JAAAARUxFVkFUSU9OAAAAav///xQAAAAUAAAAFAAAAAAABQEQAAAAAAAAAAAAAABY////CQAAAExPTkdJVFVERQAAAJ7///8UAAAAFAAAABQAAAAAAAUBEAAAAAAAAAAAAAAAjP///wgAAABMQVRJVFVERQAAAADS////FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAMD///8EAAAATkFNRQAAEgAYABQAEwASAAwAAAAIAAQAEgAAABQAAAAUAAAAGAAAAAAABQEUAAAAAAAAAAAAAAAEAAQABAAAAAcAAABTVEFUSU9OAA==");
    dp2.setSql("");
    String readSchema = serde.readSchema(dp2);
    assertThat(readSchema.replace("\r", "")) // normalize for windows
        .isEqualTo(
            "CREATE TABLE test.my.path as \n"
                + "SELECT"
                + " \"STATION\",\"NAME\",\"LATITUDE\",\"LONGITUDE\",\"ELEVATION\",\"DATE\",\"PRCP\",\"SNOW\",\"SNWD\",\"TAVG\",\"TMAX\",\"TMIN\""
                + " \n"
                + "FROM (values ('Hello world!','Hello world!','Hello world!','Hello world!','Hello"
                + " world!','Hello world!','Hello world!','Hello world!','Hello world!','Hello"
                + " world!','Hello world!','Hello world!'),\n"
                + "('Hello world!','Hello world!','Hello world!','Hello world!','Hello"
                + " world!','Hello world!','Hello world!','Hello world!','Hello world!','Hello"
                + " world!','Hello world!','Hello world!')) as"
                + " t(\"STATION\",\"NAME\",\"LATITUDE\",\"LONGITUDE\",\"ELEVATION\",\"DATE\",\"PRCP\",\"SNOW\",\"SNWD\",\"TAVG\",\"TMAX\",\"TMIN\");");
  }

  @Test
  void testColumnOverrides() {
    ColumnDefYaml overrides = new ColumnDefYaml();
    List<TableDef> tableDefs = new ArrayList<>();
    TableDef tableDef = new TableDef();
    String tableName = "test.my.path";
    tableDef.setName(tableName);
    ColumnDef column = new ColumnDef();
    column.setName("STATION");
    column.setValues(Collections.singletonList("myStation"));
    List<ColumnDef> columns = Collections.singletonList(column);
    tableDef.setColumns(columns);
    tableDefs.add(tableDef);
    overrides.setTables(tableDefs);
    final ArrowFormatDeserializer serdeWithOverrides =
        new ArrowFormatDeserializer(2L, dp, dp, overrides);
    DatasetProfile dp2 = new DatasetProfile();
    dp2.setType(1);
    dp2.setDatasetPath(tableName);
    dp2.setBatchSchema(
        "EAAAAAAACgAMAAAACAAEAAoAAAAIAAAACAAAAAAAAAANAAAArAIAAGgCAAAwAgAA+AEAAMABAACMAQAAWAEAACQBAADwAAAAvAAAAIgAAABUAAAABAAAAJr9//8UAAAAFAAAABwAAAAAAAIBIAAAAAAAAAAAAAAACAAMAAgABwAIAAAAAAAAAUAAAAATAAAAJF9kcmVtaW9fJF91cGRhdGVfJADm/f//FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAANT9//8EAAAAVE1JTgAAAAAW/v//FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAAT+//8EAAAAVE1BWAAAAABG/v//FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAADT+//8EAAAAVEFWRwAAAAB2/v//FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAGT+//8EAAAAU05XRAAAAACm/v//FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAJT+//8EAAAAU05PVwAAAADW/v//FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAMT+//8EAAAAUFJDUAAAAAAG////FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAPT+//8EAAAAREFURQAAAAA2////FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAACT///8JAAAARUxFVkFUSU9OAAAAav///xQAAAAUAAAAFAAAAAAABQEQAAAAAAAAAAAAAABY////CQAAAExPTkdJVFVERQAAAJ7///8UAAAAFAAAABQAAAAAAAUBEAAAAAAAAAAAAAAAjP///wgAAABMQVRJVFVERQAAAADS////FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAMD///8EAAAATkFNRQAAEgAYABQAEwASAAwAAAAIAAQAEgAAABQAAAAUAAAAGAAAAAAABQEUAAAAAAAAAAAAAAAEAAQABAAAAAcAAABTVEFUSU9OAA==");
    dp2.setSql("");
    String schema = serdeWithOverrides.readSchema(dp2).replace("\r", "");
    assertThat(schema)
        .isEqualTo(
            "CREATE TABLE test.my.path as \n"
                + "SELECT"
                + " \"STATION\",\"NAME\",\"LATITUDE\",\"LONGITUDE\",\"ELEVATION\",\"DATE\",\"PRCP\",\"SNOW\",\"SNWD\",\"TAVG\",\"TMAX\",\"TMIN\""
                + " \n"
                + "FROM (values ('myStation','Hello world!','Hello world!','Hello world!','Hello"
                + " world!','Hello world!','Hello world!','Hello world!','Hello world!','Hello"
                + " world!','Hello world!','Hello world!'),\n"
                + "('myStation','Hello world!','Hello world!','Hello world!','Hello world!','Hello"
                + " world!','Hello world!','Hello world!','Hello world!','Hello world!','Hello"
                + " world!','Hello world!')) as"
                + " t(\"STATION\",\"NAME\",\"LATITUDE\",\"LONGITUDE\",\"ELEVATION\",\"DATE\",\"PRCP\",\"SNOW\",\"SNWD\",\"TAVG\",\"TMAX\",\"TMIN\");");
  }

  @Test
  void testSupportsString() {
    DictionaryEncoding dict = new DictionaryEncoding(1L, false, new Int(64, false));
    Field f = new Field(tester, new FieldType(true, new ArrowType.Utf8(), dict), new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("'Hello world!'");
  }

  @Test
  void testSupportsLargeUtf8String() {
    DictionaryEncoding dict = new DictionaryEncoding(1L, false, new Int(64, false));
    Field f =
        new Field(tester, new FieldType(true, new ArrowType.LargeUtf8(), dict), new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("'Hello world!'");
  }

  @Test
  void testSupportsLargeUtf8StringAsNull() {
    DictionaryEncoding dict = new DictionaryEncoding(1L, false, new Int(64, false));
    Field f =
        new Field(tester, new FieldType(true, new ArrowType.LargeUtf8(), dict), new ArrayList<>());
    String data = serde.fieldToData(f, emptyDP);
    assertThat(data).isEqualTo("cast(null as VARCHAR(65536))");
  }

  @Test
  void testSupportsBoolean() {
    DictionaryEncoding dict = new DictionaryEncoding(1L, false, new Int(64, false));
    Field f = new Field(tester, new FieldType(true, new ArrowType.Bool(), dict), new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("true");
  }

  @Test
  void testSupportsInt() {
    DictionaryEncoding dict = new DictionaryEncoding(1L, false, new Int(32, false));
    Field f =
        new Field(
            tester, new FieldType(true, new ArrowType.Int(32, false), dict), new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("cast(1 as INTEGER)");
  }

  @Test
  void testSupportsBigInt() {
    DictionaryEncoding dict = new DictionaryEncoding(1L, false, new Int(64, false));
    final Field f =
        new Field(
            tester, new FieldType(true, new ArrowType.Int(64, false), dict), new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("cast(1 as BIGINT)");
  }

  @Test
  void testSupportsLong() {
    DictionaryEncoding dict = new DictionaryEncoding(1L, false, new Int(64, false));
    final Field f =
        new Field(
            tester, new FieldType(true, new ArrowType.Int(64, false), dict), new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("cast(1 as BIGINT)");
  }

  @Test
  void testSupportsDouble() {
    DictionaryEncoding dict = new DictionaryEncoding(1L, false, new Int(64, false));
    final Field f =
        new Field(
            tester,
            new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), dict),
            new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("cast(1.0 as DOUBLE)");
  }

  @Test
  void testSupportsFloat() {
    DictionaryEncoding dict = new DictionaryEncoding(1L, false, new Int(64, false));
    final Field f =
        new Field(
            tester,
            new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), dict),
            new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("cast(1.0 as FLOAT)");
  }

  @Test
  void testDecimal() {
    DictionaryEncoding dict = new DictionaryEncoding(1L, false, new Int(64, false));
    final Field f =
        new Field(
            tester,
            new FieldType(true, new ArrowType.Decimal(20, 10, 128), dict),
            new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("cast(1.0 as DECIMAL(20,10))");
  }

  @Test
  void testBinary() {
    final ArrowType.Binary arrowType = new ArrowType.Binary();
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field f = new Field("fieldname", fieldType, new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("BINARY_STRING('Hello world!')");
  }

  @Test
  void testDate() {
    final ArrowType.Date arrowType = new ArrowType.Date(DateUnit.MILLISECOND);
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field f = new Field("fieldname", fieldType, new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("cast('2018-09-14' as DATE)");
  }

  @Test
  void testTime() {
    final ArrowType.Time arrowType = new ArrowType.Time(TimeUnit.MILLISECOND, 128);
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field f = new Field("fieldname", fieldType, new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("cast('00:00:00.000' as TIME)");
  }

  @Test
  void testTimestamp() {
    final ArrowType.Timestamp arrowType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field f = new Field("fieldname", fieldType, new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("cast('2022-09-15 00:00:00.000' AS TIMESTAMP)");
  }

  @Test
  void testInterval() {
    final ArrowType.Interval arrowType = new ArrowType.Interval(IntervalUnit.DAY_TIME);
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field f = new Field("fieldname", fieldType, new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("INTERVAL '1' DAY");
  }

  @Test
  void testNull() {
    final ArrowType.Null arrowType = new ArrowType.Null();
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field f = new Field("fieldname", fieldType, new ArrayList<>());
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("null");
  }

  @Test
  void testBinaryNull() {
    final ArrowType.Binary arrowType = new ArrowType.Binary();
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field f = new Field("fieldname", fieldType, new ArrayList<>());
    String data = serde.fieldToData(f, emptyDP);
    assertThat(data).isEqualTo("null");
  }

  @Test
  void testList() {
    DictionaryEncoding dict = new DictionaryEncoding(1L, false, new Int(64, false));
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.Int arrowType = new ArrowType.Int(32, true);
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field field = new Field("fieldname", fieldType, new ArrayList<>());
    elementTypes.add(field);
    final Field f =
        new Field(tester, new FieldType(true, new ArrowType.List(), dict), elementTypes);
    String data = serde.fieldToData(f, dp);
    assertThat(data).isEqualTo("CONVERT_FROM('[{ \"$numberInt\": 1}]', 'json')");
  }

  @Test
  void testStructure() {
    DictionaryEncoding dict = new DictionaryEncoding(1L, false, new Int(64, false));
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.Int arrowType = new ArrowType.Int(32, true);
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field field = new Field("fieldname", fieldType, new ArrayList<>());
    elementTypes.add(field);
    final ArrowType.Timestamp arrowType2 = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
    final FieldType fieldType2 = FieldType.nullable(arrowType2);
    final Field field2 = new Field("fieldname2", fieldType2, new ArrayList<>());
    elementTypes.add(field2);
    final Field f =
        new Field(tester, new FieldType(true, new ArrowType.Struct(), dict), elementTypes);
    String data = serde.fieldToData(f, dp);
    assertThat(data)
        .isEqualTo(
            "CONVERT_FROM('{\"fieldname\": { \"$numberInt\": 1}, \"fieldname2\": { \"$date\":"
                + " \"2022-09-15T00:00:00.000Z\"}}', 'json')");
  }
}
