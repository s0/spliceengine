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

package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Tests cursors
 */
public class CursorTest extends BaseJDBCTestCase {

    /**
     * Creates a new <code>CursorTest</code> instance.
     * 
     * @param name
     *            name of the test
     */
    public CursorTest(String name) {
        super(name);
    }

    /**
     * Test cursor methods with regular cursor
     * 
     * @throws SQLException
     */
    public void testCursor() throws SQLException {
        PreparedStatement select;
        ResultSet cursor;

        // because there is no order by (nor can there be)
        // the fact that this test prints out rows may someday
        // be a problem. When that day comes, the row printing
        // can (should) be removed from this test.

        select = prepareStatement("select i, c from t for update");
        cursor = select.executeQuery(); // cursor is now open
        cursor.next();
        assertEquals(1956, cursor.getInt(1));
        assertEquals("hello world", cursor.getString(2).trim());
        // close and then test that fetch causes an error
        cursor.close();
        if (usingEmbedded()) {
            assertNextError("XCL16", cursor);
        } else if (usingDerbyNetClient()) {
            assertNextError("XCL16", cursor);
        }
        // restart the query for another test
        cursor = select.executeQuery();
        // test next/getInt past the end of table
        while (cursor.next())
            ;
        cursor.next();
        if (usingEmbedded()) {
            assertGetIntError(1, "24000", cursor);
        } else if (usingDerbyNetClient()) {
            assertGetIntError(1, "XJ121", cursor);
        }
        cursor.close();
        }

    /**
     * Test cursor methods with parameter
     * 
     * @throws SQLException
     */
    public void testCursorParam() throws SQLException {
        PreparedStatement select;
        ResultSet cursor;
        select = prepareStatement("select i, c from t where ?=1 for update");
        select.setInt(1, 1);
        cursor = select.executeQuery();
        // TEST: fetch of a row works
        assertTrue("FAIL: unable to fetch row.", cursor.next());
        assertEquals("FAIL: Wrong row on fetch with param", 1956, cursor
                .getInt(1));
        // TEST: Close and then fetch gets an error on fetch.
        cursor.close();
        assertNextError("XCL16", cursor);
        // restart the query for another test
        select.setBoolean(1, false);
        select.setCursorName("ForCoverageSake");
        cursor = select.executeQuery();
        assertEquals("ForCoverageSake", cursor.getCursorName());
        cursor.next();
        if (usingEmbedded()) {
            assertGetIntError(1, "24000", cursor);
        } else if (usingDerbyNetClient()) {
            assertGetIntError(1, "XJ121", cursor);
        }
        cursor.close();
    }

    /**
     * Test getCursorName with and without update cursor
     * 
     * @throws SQLException
     */
    public void testGetCursorName() throws SQLException {
        Statement s = createStatement();
        ResultSet rs = s.executeQuery("select * from t");
        if (usingEmbedded())
            assertNull(rs.getCursorName());
        else if (usingDerbyNetClient())
            assertNotNull(rs.getCursorName());
        rs.close();

        // test Automatic naming of cursor for update
        rs = s.executeQuery("select * from t for update");
        assertNotNull( rs.getCursorName());
         rs.close();
        
        rs = s.executeQuery("select * from t for update of i");
        assertNotNull(rs.getCursorName());
        rs.close();
        

        s.setCursorName("myselect");
        rs = s.executeQuery("select * from t");
        assertEquals("myselect", rs.getCursorName());
        rs.close();

        s.setCursorName("myselect2");
        rs = s.executeQuery("select * from t for update");
        assertEquals("myselect2", rs.getCursorName());
        rs.close();
        
        s.setCursorName("myselect3");
        rs = s.executeQuery("select * from t for update of i");
        assertEquals("myselect3", rs.getCursorName());
        rs.close();
        s.close();
    }
    
    /**
     * Test that Statement.setCursorName affects only the next
     * execution and not any open ResultSet.
     */
    public void derby2417testSetCursorNextExecute() throws SQLException
    {   
        // Assert setCursorName only affects the next execution.
        // For statements
        Statement s = createStatement();
        ResultSet rs = s.executeQuery("select * from t for update");
        s.setCursorName("AFTER_EXECUTE");
        assertFalse("AFTER_EXECUTE".equals(rs.getCursorName()));
        rs.close();
        rs = s.executeQuery("select * from t");
        assertEquals("AFTER_EXECUTE", rs.getCursorName());
        s.setCursorName("CHANGE_AGAIN");
        assertEquals("AFTER_EXECUTE", rs.getCursorName());
        rs.close();
        rs = s.executeQuery("select * from t");
        assertEquals("CHANGE_AGAIN", rs.getCursorName());
        s.close();
        
        // And prepared statements
        PreparedStatement ps = prepareStatement("select * from t for update");
        rs = ps.executeQuery();
        ps.setCursorName("AFTER_EXECUTE");
        assertFalse("AFTER_EXECUTE".equals(rs.getCursorName()));
        rs.close();
        rs = ps.executeQuery();
        assertEquals("AFTER_EXECUTE", rs.getCursorName());
        ps.setCursorName("CHANGE_AGAIN");
        assertEquals("AFTER_EXECUTE", rs.getCursorName());
        rs.close();
        rs = ps.executeQuery();
        assertEquals("CHANGE_AGAIN", rs.getCursorName());
        ps.close();        
   
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("CursorTest");
        
        suite.addTest(TestConfiguration.defaultSuite(CursorTest.class));
        
        // DERBY-2417 client throws exception for setCursorName
        // on Statement with active ResultSet.
        suite.addTest(new CursorTest("derby2417testSetCursorNextExecute"));
        
        return suite;
    }

    protected void setUp() throws SQLException {
        getConnection().setAutoCommit(false);
        Statement stmt = createStatement();
        stmt.executeUpdate("create table t (i int, c char(50))");
        stmt.executeUpdate("create table s (i int, c char(50))");
        stmt.executeUpdate("insert into t values (1956, 'hello world')");
        stmt.executeUpdate("insert into t values (456, 'hi yourself')");
        stmt.executeUpdate("insert into t values (180, 'rubber ducky')");
        stmt.executeUpdate("insert into t values (3, 'you are the one')");

        stmt.close();
        commit();
    }

    protected void tearDown() throws Exception {
        Statement stmt = createStatement();
        rollback();
        stmt.executeUpdate("drop table t");
        stmt.executeUpdate("drop table s");
        stmt.close();
        commit();
        super.tearDown();
    }

}
