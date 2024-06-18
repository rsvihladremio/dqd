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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dremio.support.diagnostics.repro.PDSDataProvider;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

class JsonFormatterTest {

  static class PDSTestDataProvider implements PDSDataProvider {

    private long longField;
    private float floatField;
    private double doubleField;
    private int intField;
    private String stringField;
    private LocalDate localDateField;
    private Instant instantField;
    private LocalTime localTimeField;
    private boolean boolField;
    private List<String> listField;

    public void setLongField(final long longField) {
      this.longField = longField;
    }

    public void setFloatField(final float floatField) {
      this.floatField = floatField;
    }

    public void setDoubleField(final double doubleField) {
      this.doubleField = doubleField;
    }

    public void setIntField(final int intField) {
      this.intField = intField;
    }

    public void setStringField(final String stringField) {
      this.stringField = stringField;
    }

    public void setLocalDateField(final LocalDate localDateField) {
      this.localDateField = localDateField;
    }

    public void setInstantField(final Instant instantField) {
      this.instantField = instantField;
    }

    public void setLocalTimeField(final LocalTime localTimeField) {
      this.localTimeField = localTimeField;
    }

    public void setBoolField(final boolean boolField) {
      this.boolField = boolField;
    }

    @Override
    public long getLong() {
      return longField;
    }

    @Override
    public float getFloat() {
      return floatField;
    }

    @Override
    public double getDouble() {
      return doubleField;
    }

    @Override
    public int getInt() {
      return intField;
    }

    @Override
    public String getString() {
      return stringField;
    }

    @Override
    public LocalDate getLocalDate() {
      return localDateField;
    }

    @Override
    public Instant getInstant() {
      return instantField;
    }

    @Override
    public LocalTime getTime() {
      return localTimeField;
    }

    @Override
    public boolean getBoolean() {
      return boolField;
    }

    @Override
    public List<String> getList() {
      return listField;
    }

    public void setListField(final List<String> listField) {
      this.listField = listField;
    }

    @Override
    public String getInterval() {
      return String.format("INTERVAL '%d' DAY", intField);
    }
  }

  private static final String placeHolder = "not sure what goes here";

  @Test
  void testGenerateAStruct() {
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.Int arrowIntType = new ArrowType.Int(32, true);
    final FieldType intFieldType = FieldType.nullable(arrowIntType);
    final Field intField = new Field("field1", intFieldType, elementTypes);
    elementTypes.add(intField);
    final ArrowType.Int arrowLongType = new ArrowType.Int(64, true);
    final FieldType longFieldType = FieldType.nullable(arrowLongType);
    final Field longField = new Field("field2", longFieldType, elementTypes);
    elementTypes.add(longField);
    final ArrowType.Utf8 arrowStringType = new ArrowType.Utf8();
    final FieldType stringFieldType = FieldType.nullable(arrowStringType);
    final Field stringField = new Field("field3", stringFieldType, elementTypes);
    elementTypes.add(stringField);
    final ArrowType.Struct arrowStructType = new ArrowType.Struct();
    final FieldType fieldType = FieldType.nullable(arrowStructType);
    final Field field = new Field("myStruct", fieldType, elementTypes);

    final JsonFormatter jsonFormatter = new JsonFormatter();
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(1);
    dataProvider.setLongField(2);
    dataProvider.setStringField("element");
    final String jsonString = jsonFormatter.getJsonStringFromField(field, dataProvider);
    assertEquals(
        "{\"field1\": { \"$numberInt\": 1}, \"field2\": { \"$numberLong\": 2}, \"field3\":"
            + " \"element\"}",
        jsonString,
        "ints do not match");
  }

  @Test
  void testGenerateInvalidBitWidthForint() {
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.Int arrowIntType = new ArrowType.Int(128, true);
    final FieldType intFieldType = FieldType.nullable(arrowIntType);
    final Field field = new Field(placeHolder, intFieldType, elementTypes);
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(2);
    final JsonFormatter jsonFormatter = new JsonFormatter();
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> jsonFormatter.getJsonStringFromField(field, dataProvider));
    assertEquals("Unsupported bitWidth: 128", thrown.getMessage());
  }

  @Test
  void testGenerateListOfInts() {
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.Int arrowIntType = new ArrowType.Int(32, true);
    final FieldType intFieldType = FieldType.nullable(arrowIntType);
    final Field intField = new Field(placeHolder, intFieldType, elementTypes);
    elementTypes.add(intField);
    final ArrowType.List arrowListType = new ArrowType.List();
    final FieldType fieldType = FieldType.nullable(arrowListType);
    final Field field = new Field("myIntList", fieldType, elementTypes);

    final JsonFormatter jsonFormatter = new JsonFormatter();
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(2);
    dataProvider.setStringField("element");
    final String jsonString = jsonFormatter.getJsonStringFromField(field, dataProvider);
    assertEquals(
        "[{ \"$numberInt\": 2}, { \"$numberInt\": 2}]", jsonString, "list of ints do not match");
  }

  @Test
  void testGenerateListOfLongs() {
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.Int arrowLongType = new ArrowType.Int(64, true);
    final FieldType longType = FieldType.nullable(arrowLongType);
    final Field longField = new Field(placeHolder, longType, elementTypes);
    elementTypes.add(longField);
    final ArrowType.List arrowListType = new ArrowType.List();
    final FieldType fieldType = FieldType.nullable(arrowListType);
    final Field field = new Field("myLongList", fieldType, elementTypes);

    final JsonFormatter jsonFormatter = new JsonFormatter();
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(2);
    dataProvider.setLongField(3L);
    dataProvider.setStringField("element");
    final String jsonString = jsonFormatter.getJsonStringFromField(field, dataProvider);
    assertEquals(
        "[{ \"$numberLong\": 3}, { \"$numberLong\": 3}]", jsonString, "list of longs do not match");
  }

  @Test
  void testGenerateListOfDecimals() {
    final List<Field> elementTypes = new ArrayList<>();
    @SuppressWarnings("deprecation")
    final ArrowType.Decimal arrowDecimalType = new ArrowType.Decimal(64, 2);
    final FieldType decimalType = FieldType.nullable(arrowDecimalType);
    final Field decimalField = new Field(placeHolder, decimalType, elementTypes);
    elementTypes.add(decimalField);
    final ArrowType.List arrowListType = new ArrowType.List();
    final FieldType fieldType = FieldType.nullable(arrowListType);
    final Field field = new Field("myDecimalList", fieldType, elementTypes);

    final JsonFormatter jsonFormatter = new JsonFormatter();
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(2);
    dataProvider.setDoubleField(0.20);
    final String jsonString = jsonFormatter.getJsonStringFromField(field, dataProvider);
    assertEquals(
        "[{ \"$numberDecimal\": 0.20}, { \"$numberDecimal\": 0.20}]",
        jsonString,
        "list of decimals do not match");
  }

  @Test
  void testGenerateListOfString() {
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.Utf8 arrowUtf8Type = new ArrowType.Utf8();
    final FieldType utf8Type = FieldType.nullable(arrowUtf8Type);
    final Field utf8Field = new Field(placeHolder, utf8Type, elementTypes);
    elementTypes.add(utf8Field);
    final ArrowType.List arrowListType = new ArrowType.List();
    final FieldType fieldType = FieldType.nullable(arrowListType);
    final Field field = new Field("myStringList", fieldType, elementTypes);

    final JsonFormatter jsonFormatter = new JsonFormatter();
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(2);
    dataProvider.setStringField("my string");
    final String jsonString = jsonFormatter.getJsonStringFromField(field, dataProvider);
    assertEquals("[\"my string\", \"my string\"]", jsonString, "list of strings do not match");
  }

  @Test
  void testGenerateListOfBinaries() {
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.LargeBinary arrowLargeBinaryType = new ArrowType.LargeBinary();
    final FieldType largeBinaryType = FieldType.nullable(arrowLargeBinaryType);
    final Field largeBinaryField = new Field(placeHolder, largeBinaryType, elementTypes);
    elementTypes.add(largeBinaryField);
    final ArrowType.List arrowListType = new ArrowType.List();
    final FieldType fieldType = FieldType.nullable(arrowListType);
    final Field field = new Field("myBinaryList", fieldType, elementTypes);

    final JsonFormatter jsonFormatter = new JsonFormatter();
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(2);
    dataProvider.setStringField("my string");
    final String jsonString = jsonFormatter.getJsonStringFromField(field, dataProvider);
    assertEquals(
        "[\"bXkgc3RyaW5n\", \"bXkgc3RyaW5n\"]", jsonString, "list of strings do not match");
  }

  @Test
  void testGenerateListOfTimes() {
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.Time arrowTimeType = new ArrowType.Time(TimeUnit.MILLISECOND, 32);
    final FieldType timeType = FieldType.nullable(arrowTimeType);
    final Field arrowField = new Field(placeHolder, timeType, elementTypes);
    elementTypes.add(arrowField);
    final ArrowType.List arrowListType = new ArrowType.List();
    final FieldType fieldType = FieldType.nullable(arrowListType);
    final Field field = new Field("myTimeList", fieldType, elementTypes);

    final JsonFormatter jsonFormatter = new JsonFormatter();
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(2);
    dataProvider.setLocalTimeField(LocalTime.of(16, 10));
    final String jsonString = jsonFormatter.getJsonStringFromField(field, dataProvider);
    assertEquals(
        "[{ \"$date\": \"16:10:00.000\"}, { \"$date\": \"16:10:00.000\"}]",
        jsonString,
        "list of times do not match");
  }

  @Test
  void testGenerateListOfDates() {
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.Date arrowType = new ArrowType.Date(DateUnit.MILLISECOND);
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field arrowField = new Field(placeHolder, fieldType, elementTypes);
    elementTypes.add(arrowField);
    final ArrowType.List arrowListType = new ArrowType.List();
    final FieldType listFieldType = FieldType.nullable(arrowListType);
    final Field field = new Field("myDateList", listFieldType, elementTypes);

    final JsonFormatter jsonFormatter = new JsonFormatter();
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(2);
    dataProvider.setLocalDateField(LocalDate.of(2011, 10, 30));
    final String jsonString = jsonFormatter.getJsonStringFromField(field, dataProvider);
    assertEquals(
        "[{ \"$date\": \"2011-10-30T00:00:00.000Z\"}, { \"$date\": \"2011-10-30T00:00:00.000Z\"}]",
        jsonString,
        "list of dates do not match");
  }

  @Test
  void testGenerateListOfTimestamps() {
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.Timestamp arrowType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field arrowField = new Field(placeHolder, fieldType, elementTypes);
    elementTypes.add(arrowField);
    final ArrowType.List arrowListType = new ArrowType.List();
    final FieldType listFieldType = FieldType.nullable(arrowListType);
    final Field field = new Field("myTimestampList", listFieldType, elementTypes);

    final JsonFormatter jsonFormatter = new JsonFormatter();
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(2);
    dataProvider.setInstantField(Instant.parse("2011-10-30T00:34:10.001Z"));
    final String jsonString = jsonFormatter.getJsonStringFromField(field, dataProvider);
    assertEquals(
        "[{ \"$date\": \"2011-10-30T00:34:10.001Z\"}, { \"$date\": \"2011-10-30T00:34:10.001Z\"}]",
        jsonString,
        "list of timestamps do not match");
  }

  @Test
  void testGenerateListOfNulls() {
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.Null arrowType = new ArrowType.Null();
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field arrowField = new Field(placeHolder, fieldType, elementTypes);
    elementTypes.add(arrowField);
    final ArrowType.List arrowListType = new ArrowType.List();
    final FieldType listFieldType = FieldType.nullable(arrowListType);
    final Field field = new Field("myNullList", listFieldType, elementTypes);

    final JsonFormatter jsonFormatter = new JsonFormatter();
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(2);
    final String jsonString = jsonFormatter.getJsonStringFromField(field, dataProvider);
    assertEquals("[null, null]", jsonString, "list of nulls do not match");
  }

  @Test
  void testGenerateListOfBools() {
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.Bool arrowType = new ArrowType.Bool();
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field arrowField = new Field(placeHolder, fieldType, elementTypes);
    elementTypes.add(arrowField);
    final ArrowType.List arrowListType = new ArrowType.List();
    final FieldType listFieldType = FieldType.nullable(arrowListType);
    final Field field = new Field("myBoolList", listFieldType, elementTypes);

    final JsonFormatter jsonFormatter = new JsonFormatter();
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(2);
    dataProvider.setBoolField(true);
    final String jsonString = jsonFormatter.getJsonStringFromField(field, dataProvider);
    assertEquals("[true, true]", jsonString, "list of bools do not match");
  }

  @Test
  void testGenerateListOfFloats() {
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.FloatingPoint arrowType =
        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field arrowField = new Field(placeHolder, fieldType, elementTypes);
    elementTypes.add(arrowField);
    final ArrowType.List arrowListType = new ArrowType.List();
    final FieldType listFieldType = FieldType.nullable(arrowListType);
    final Field field = new Field("myFloatList", listFieldType, elementTypes);

    final JsonFormatter jsonFormatter = new JsonFormatter();
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(2);
    dataProvider.setFloatField(9.0f);
    final String jsonString = jsonFormatter.getJsonStringFromField(field, dataProvider);
    assertEquals(
        "[{ \"$numberDouble\": 9.0000}, { \"$numberDouble\": 9.0000}]",
        jsonString,
        "list of floats do not match");
  }

  @Test
  void testGenerateListOfDoubles() {
    final List<Field> elementTypes = new ArrayList<>();
    FloatingPointPrecision precision = FloatingPointPrecision.DOUBLE;
    final ArrowType.FloatingPoint arrowType = new ArrowType.FloatingPoint(precision);
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field arrowField = new Field(placeHolder, fieldType, elementTypes);
    elementTypes.add(arrowField);
    final ArrowType.List arrowListType = new ArrowType.List();
    final FieldType listFieldType = FieldType.nullable(arrowListType);
    final Field field = new Field("myDoubleList", listFieldType, elementTypes);

    final JsonFormatter jsonFormatter = new JsonFormatter();
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(2);
    dataProvider.setDoubleField(2.0d);
    final String jsonString = jsonFormatter.getJsonStringFromField(field, dataProvider);
    assertEquals(
        "[{ \"$numberDouble\": 2.0000}, { \"$numberDouble\": 2.0000}]",
        jsonString,
        "list of doubles do not match");
  }

  @Test
  void testGenerateInvalidPrecisionForFloat() {
    final List<Field> elementTypes = new ArrayList<>();
    final ArrowType.FloatingPoint arrowType =
        new ArrowType.FloatingPoint(FloatingPointPrecision.HALF);
    final FieldType fieldType = FieldType.nullable(arrowType);
    final Field field = new Field(placeHolder, fieldType, elementTypes);
    final PDSTestDataProvider dataProvider = new PDSTestDataProvider();
    dataProvider.setIntField(2);
    dataProvider.setFloatField(20.0f);
    final JsonFormatter jsonFormatter = new JsonFormatter();
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> jsonFormatter.getJsonStringFromField(field, dataProvider));
    assertEquals("Unsupported precision: " + FloatingPointPrecision.HALF, thrown.getMessage());
  }
}
