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
package com.splicemachine.dbTesting.junit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

import javax.sql.ConnectionPoolDataSource;

import junit.framework.AssertionFailedError;

/**
 * Connection factory using javax.sql.ConnectionPoolDataSource.
 * <p>
 * Connections are obtained by calling
 * <code>getPooledConnection.getConnection()</code>, and statement pooling is
 * enabled.
 */
public class ConnectionPoolDataSourceConnector implements Connector {
    
    private TestConfiguration config;
    /**
     * DataSource that maps to the database for the
     * configuration. The no-arg getPooledConnection() method
     * maps to the default user and password for the
     * configuration.
     */
    private ConnectionPoolDataSource ds;

    public void setConfiguration(TestConfiguration config) {
        
        this.config = config;
        ds = J2EEDataSource.getConnectionPoolDataSource(config, (HashMap) null);
        // Enable statement pooling by default.
        // Note that this does not automatically test the pooling itself, but it
        // should test basic JDBC operations on the logical wrapper classes.
        try {
            J2EEDataSource.setBeanProperty(ds, "maxStatements", new Integer(2));
        } catch (AssertionFailedError afe) {
            // Ignore this, it will fail later if it is an actual error.
            // An assertion error will be thrown every time until statement
            // pooling (or merely the property maxStatement) is implemented in
            // the embedded ConnectionPoolDataSource class.
        }
    }

    public Connection openConnection() throws SQLException {
        try {
            return ds.getPooledConnection().getConnection();
        } catch (SQLException e) {
            // Expected state for database not found.
            // For the client the generic 08004 is returned,
            // will just retry on that.
            String expectedState = 
                config.getJDBCClient().isEmbedded() ? "XJ004" : "08004";

            // If there is a database not found exception
            // then retry the connection request with
            // a new DataSource with the createDtabase property set.
            if (!expectedState.equals(e.getSQLState()))
                throw e;
            return singleUseDS( DataSourceConnector.makeCreateDBAttributes( config ) ).
                   getPooledConnection().getConnection(); 
       }
    }

    public Connection openConnection(String databaseName) throws SQLException {
        JDBCDataSource.setBeanProperty(ds, "databaseName", databaseName);
        try {
            return ds.getPooledConnection().getConnection();
        } catch (SQLException e) {
            // Expected state for database not found.
            // For the client the generic 08004 is returned,
            // will just retry on that.
            String expectedState = 
                config.getJDBCClient().isEmbedded() ? "XJ004" : "08004";

            // If there is a database not found exception
            // then retry the connection request with
            // a new DataSource with the createDtabase property set.
            if (!expectedState.equals(e.getSQLState()))
                throw e;
            ConnectionPoolDataSource tmpDs =
                    singleUseDS( DataSourceConnector.makeCreateDBAttributes( config ) );
            JDBCDataSource.setBeanProperty(tmpDs, "databaseName", databaseName);
            return tmpDs.getPooledConnection().getConnection();
       }
    }

    public Connection openConnection(String user, String password)
            throws SQLException {
        try {
            return ds.getPooledConnection(user, password).getConnection();
        } catch (SQLException e) {
            // If there is a database not found exception
            // then retry the connection request with
            // a new DataSource with the createDatabase property set.
            if (!"XJ004".equals(e.getSQLState()))
                throw e;
            return singleUseDS( DataSourceConnector.makeCreateDBAttributes( config ) ).
                   getPooledConnection(user, password).getConnection(); 
       }
    }

    public Connection openConnection(String databaseName,
                                     String user,
                                     String password)
            throws SQLException
    {
        return openConnection( databaseName, user, password, null );
    }
    
    public  Connection openConnection
        (String databaseName, String user, String password, Properties connectionProperties)
         throws SQLException
    {
        JDBCDataSource.setBeanProperty(ds, "databaseName", databaseName);
        try {
            return ds.getPooledConnection(user, password).getConnection();
        } catch (SQLException e) {
            // If there is a database not found exception
            // then retry the connection request with
            // a new DataSource with the createDatabase property set.
            if (!"XJ004".equals(e.getSQLState()))
                throw e;
            HashMap hm = DataSourceConnector.makeCreateDBAttributes( config );
            if ( connectionProperties != null ) { hm.putAll( connectionProperties ); }
            ConnectionPoolDataSource tmpDs = singleUseDS( hm );
            JDBCDataSource.setBeanProperty(tmpDs, "databaseName", databaseName);
            return tmpDs.getPooledConnection(user, password).getConnection(); 
       }
    }

    public void shutDatabase() throws SQLException {
        singleUseDS( DataSourceConnector.makeShutdownDBAttributes( config ) ).
                getPooledConnection().getConnection();     
    }

    public void shutEngine() throws SQLException {
        ConnectionPoolDataSource tmpDs =
                singleUseDS( DataSourceConnector.makeShutdownDBAttributes( config ) );
        JDBCDataSource.setBeanProperty(tmpDs, "databaseName", "");
        tmpDs.getPooledConnection();
    }
    
    /**
     * Get a connection from a single use ConnectionPoolDataSource configured
     * from the configuration but with the passed in property set.
     */
    private ConnectionPoolDataSource singleUseDS( HashMap hm )
       throws SQLException
    {
        return J2EEDataSource.getConnectionPoolDataSource(config, hm);
    }

}
