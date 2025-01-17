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
package com.splicemachine.dbTesting.system.oe.direct;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

import com.splicemachine.dbTesting.system.oe.model.Address;

/**
 * Utility class for direct operations. Maintains the connection
 * and a hash table of PreparedStatements to simplify management
 * of PreparedStatement objects by the business transactions.
 *
 */
class StatementHelper {

    protected final Connection conn;
    
    StatementHelper(Connection conn, boolean autoCommit, int isolation) throws SQLException
    {
        this.conn = conn;
        conn.setAutoCommit(autoCommit);
        conn.setTransactionIsolation(isolation);
    }
    
    /**
     * Map of SQL text to its PreparedStatement.
     * This allows the SQL text to be in-line with
     * code that sets the parameters and looks at 
     * the results. Map is on the identity of the SQL
     * string which assumes they are all constants
     * (and hence interned). Assumption is that this
     * will provide for a quicker lookup than by text
     * since the statements can be many characters.
     * 
     * May also allow easier sharing with other implementations
     * such as a Java procedure which could have a different
     * prepareStatement method.
     */
    private Map statements = new IdentityHashMap();

    /**
     * Prepare a statement, looking in the map first.
     * If the statement does not exist in the map then
     * it is prepared and put into the map for future use.
     */
    protected PreparedStatement prepareStatement(String sql) throws SQLException {
        PreparedStatement ps = (PreparedStatement) statements.get(sql);
        if (ps != null)
            return ps;
        
        // Prepare all statements as forward-only, read-only, close at commit.
        ps = conn.prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
        statements.put(sql, ps);
        return ps;
    }

    public void close() throws SQLException {
              
        for (Iterator i = statements.keySet().iterator(); i.hasNext(); )
        {
            String sql = (String) i.next();
            PreparedStatement ps = (PreparedStatement) statements.get(sql);
            ps.close();
        }
    }

    /**
     * Reset a PreparedStatement. Closes its open ResultSet
     * and clears the parameters. While clearing the parameters
     * is not required since any future execution will override
     * them, it is done here to reduce the chance of errors.
     * E.g. using the wrong prepared statement for a operation
     * or not setting all the parameters.
     * It is assumed the prepared statement was just executed.
     * @throws SQLException 
     */
    protected void reset(PreparedStatement ps) throws SQLException {
        ResultSet rs = ps.getResultSet();
        if (rs != null)
            rs.close();
        ps.clearParameters();
    }

    protected Address getAddress(ResultSet rs, String firstColumnName) throws SQLException {
        return getAddress(new Address(), rs, firstColumnName);
    }
    
    /**
     * Get the address from a query against an order entry WAREHOUSE, DISTRICT
     * or CUSTOMER table.
     * 
     * @param address Object to fill in
     * @param rs ResultSet already positioned on the current row.
     * @param firstColumnName First column that makes up the address.
     * @throws SQLException
     */
    protected Address getAddress(Address address,
            ResultSet rs, String firstColumnName) throws SQLException {
        
        address.clear();
    
        int col = rs.findColumn(firstColumnName);
        address.setStreet1(rs.getString(col++));
        address.setStreet2(rs.getString(col++));
        address.setCity(rs.getString(col++));
        address.setState(rs.getString(col++));
        address.setZip(rs.getString(col));

        return address;
    }
}
