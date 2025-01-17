/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2018 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Date;
import java.util.Arrays;
import java.util.GregorianCalendar;

/**
 *
 * Test Class for SQLDate
 *
 */
public class SQLDateTest extends SQLDataValueDescriptorTest {

        @Test
        public void serdeValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                Date date = new Date(System.currentTimeMillis());
                int computeEncodedDate = SQLDate.computeEncodedDate(date);
                SQLDate value = new SQLDate(date);
                SQLDate valueA = new SQLDate();
                value.write(writer, 0);
                Assert.assertEquals("SerdeIncorrect",computeEncodedDate,row.getInt(0));
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect",date.toString(),valueA.getDate(new GregorianCalendar()).toString());
            }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLDate value = new SQLDate();
                SQLDate valueA = new SQLDate();
                value.write(writer, 0);
                Assert.assertTrue("SerdeIncorrect", row.isNullAt(0));
                value.read(row, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
            }

        @Test
        public void testArray() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLArray value = new SQLArray();
                value.setType(new SQLDate());
                value.setValue(new DataValueDescriptor[] {new SQLDate(new Date(System.currentTimeMillis())),new SQLDate(new Date(System.currentTimeMillis())),
                        new SQLDate(new Date(System.currentTimeMillis())), new SQLDate()});
                SQLArray valueA = new SQLArray();
                valueA.setType(new SQLDate());
                writer.reset();
                value.write(writer,0);
                valueA.read(row,0);
                Assert.assertTrue("SerdeIncorrect", Arrays.equals(value.value,valueA.value));

        }

        @Test
        public void testExecRowSparkRowConversion() throws StandardException {
                ValueRow execRow = new ValueRow(1);
                execRow.setRowArray(new DataValueDescriptor[]{new SQLDate(new Date(System.currentTimeMillis()))});
                Row row = execRow.getSparkRow();
                Assert.assertEquals(execRow.getColumn(1).getDate(null),row.getDate(0));
                ValueRow execRow2 = new ValueRow(1);
                execRow2.setRowArray(new DataValueDescriptor[]{new SQLDate()});
                execRow2.getColumn(1).setSparkObject(row.get(0));
                Assert.assertEquals("ExecRow Mismatch",execRow,execRow2);
        }

}
