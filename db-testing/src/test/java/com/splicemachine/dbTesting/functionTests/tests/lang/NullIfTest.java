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

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.SQLUtilities;
import com.splicemachine.dbTesting.junit.TestConfiguration;

public class NullIfTest extends BaseJDBCTestCase {
       
        private static String[][][] nullIfResults  ={
        /*SMALLINT*/ {/*SMALLINT*/ {null,null,null,null},/*INTEGER*/ {null,null,null,"2"},/*BIGINT*/ {null,null,null,"2"},/*DECIMAL(10,5)*/ {null,null,null,"2"},/*REAL*/ {null,null,null,"2"},/*DOUBLE*/ {null,null,null,"2"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*INTEGER*/ {/*SMALLINT*/ {null,null,null,"21"},/*INTEGER*/ {null,null,null,null},/*BIGINT*/ {null,null,null,"21"},/*DECIMAL(10,5)*/ {null,null,null,"21"},/*REAL*/ {null,null,null,"21"},/*DOUBLE*/ {null,null,null,"21"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*BIGINT*/ {/*SMALLINT*/ {null,null,null,"22"},/*INTEGER*/ {null,null,null,"22"},/*BIGINT*/ {null,null,null,null},/*DECIMAL(10,5)*/ {null,null,null,"22"},/*REAL*/ {null,null,null,"22"},/*DOUBLE*/ {null,null,null,"22"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*DECIMAL(10,5)*/ {/*SMALLINT*/ {null,null,null,"23.00000"},/*INTEGER*/ {null,null,null,"23.00000"},/*BIGINT*/ {null,null,null,"23.00000"},/*DECIMAL(10,5)*/ {null,null,null,null},/*REAL*/ {null,null,null,"23.00000"},/*DOUBLE*/ {null,null,null,"23.00000"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*REAL*/ {/*SMALLINT*/ {null,null,null,"24.0"},/*INTEGER*/ {null,null,null,"24.0"},/*BIGINT*/ {null,null,null,"24.0"},/*DECIMAL(10,5)*/ {null,null,null,"24.0"},/*REAL*/ {null,null,null,null},/*DOUBLE*/ {null,null,null,"24.0"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*DOUBLE*/ {/*SMALLINT*/ {null,null,null,"25.0"},/*INTEGER*/ {null,null,null,"25.0"},/*BIGINT*/ {null,null,null,"25.0"},/*DECIMAL(10,5)*/ {null,null,null,"25.0"},/*REAL*/ {null,null,null,"25.0"},/*DOUBLE*/ {null,null,null,null},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*CHAR(60)*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {null,null,null,null},/*VARCHAR(60)*/ {null,null,null,"2.0                                                         "},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {null,"Exception","Exception","Exception"},/*TIME*/ {null,"Exception","Exception","Exception"},/*TIMESTAMP*/ {null,"Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*VARCHAR(60)*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {null,null,null,"15:30:20"},/*VARCHAR(60)*/ {null,null,null,null},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {null,"Exception","Exception","Exception"},/*TIME*/ {null,"Exception","Exception","Exception"},/*TIMESTAMP*/ {null,"Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*LONG VARCHAR*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*CHAR(60) FOR BIT DATA*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {null,null,null,null},/*VARCHAR(60) FOR BIT DATA*/ {null,null,null,"10aaaa202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*VARCHAR(60) FOR BIT DATA*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {null,null,null,"10aaba"},/*VARCHAR(60) FOR BIT DATA*/ {null,null,null,null},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*LONG VARCHAR FOR BIT DATA*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*CLOB(1k)*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*DATE*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {null,"Exception","Exception","Exception"},/*VARCHAR(60)*/ {null,"Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {null,null,null,null},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*TIME*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {null,"Exception","Exception","Exception"},/*VARCHAR(60)*/ {null,"Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {null,null,null,null},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*TIMESTAMP*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {null,"Exception","Exception","Exception"},/*VARCHAR(60)*/ {null,"Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {null,null,null,null},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
        /*BLOB(1k)*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/{"Exception","Exception","Exception","Exception"} },
        };

        // Client will error on the first row if any if DATETIME conversion exception occurs
        private static String[][][] nullIfResultsClient  ={
            /*SMALLINT*/ {/*SMALLINT*/ {null,null,null,null},/*INTEGER*/ {null,null,null,"2"},/*BIGINT*/ {null,null,null,"2"},/*DECIMAL(10,5)*/ {null,null,null,"2"},/*REAL*/ {null,null,null,"2"},/*DOUBLE*/ {null,null,null,"2"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*INTEGER*/ {/*SMALLINT*/ {null,null,null,"21"},/*INTEGER*/ {null,null,null,null},/*BIGINT*/ {null,null,null,"21"},/*DECIMAL(10,5)*/ {null,null,null,"21"},/*REAL*/ {null,null,null,"21"},/*DOUBLE*/ {null,null,null,"21"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*BIGINT*/ {/*SMALLINT*/ {null,null,null,"22"},/*INTEGER*/ {null,null,null,"22"},/*BIGINT*/ {null,null,null,null},/*DECIMAL(10,5)*/ {null,null,null,"22"},/*REAL*/ {null,null,null,"22"},/*DOUBLE*/ {null,null,null,"22"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*DECIMAL(10,5)*/ {/*SMALLINT*/ {null,null,null,"23.00000"},/*INTEGER*/ {null,null,null,"23.00000"},/*BIGINT*/ {null,null,null,"23.00000"},/*DECIMAL(10,5)*/ {null,null,null,null},/*REAL*/ {null,null,null,"23.00000"},/*DOUBLE*/ {null,null,null,"23.00000"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*REAL*/ {/*SMALLINT*/ {null,null,null,"24.0"},/*INTEGER*/ {null,null,null,"24.0"},/*BIGINT*/ {null,null,null,"24.0"},/*DECIMAL(10,5)*/ {null,null,null,"24.0"},/*REAL*/ {null,null,null,null},/*DOUBLE*/ {null,null,null,"24.0"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*DOUBLE*/ {/*SMALLINT*/ {null,null,null,"25.0"},/*INTEGER*/ {null,null,null,"25.0"},/*BIGINT*/ {null,null,null,"25.0"},/*DECIMAL(10,5)*/ {null,null,null,"25.0"},/*REAL*/ {null,null,null,"25.0"},/*DOUBLE*/ {null,null,null,null},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*CHAR(60)*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {null,null,null,null},/*VARCHAR(60)*/ {null,null,null,"2.0                                                         "},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*VARCHAR(60)*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {null,null,null,"15:30:20"},/*VARCHAR(60)*/ {null,null,null,null},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*LONG VARCHAR*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*CHAR(60) FOR BIT DATA*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {null,null,null,null},/*VARCHAR(60) FOR BIT DATA*/ {null,null,null,"10aaaa202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*VARCHAR(60) FOR BIT DATA*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {null,null,null,"10aaba"},/*VARCHAR(60) FOR BIT DATA*/ {null,null,null,null},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*LONG VARCHAR FOR BIT DATA*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*CLOB(1k)*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*DATE*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {null,null,null,null},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*TIME*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {null,null,null,null},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*TIMESTAMP*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {null,null,null,null},/*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}},
            /*BLOB(1k)*/ {/*SMALLINT*/ {"Exception","Exception","Exception","Exception"},/*INTEGER*/ {"Exception","Exception","Exception","Exception"},/*BIGINT*/ {"Exception","Exception","Exception","Exception"},/*DECIMAL(10,5)*/ {"Exception","Exception","Exception","Exception"},/*REAL*/ {"Exception","Exception","Exception","Exception"},/*DOUBLE*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60)*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},/*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},/*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},/*DATE*/ {"Exception","Exception","Exception","Exception"},/*TIME*/ {"Exception","Exception","Exception","Exception"},/*TIMESTAMP*/ {"Exception","Exception","Exception","Exception"},/*BLOB(1k)*/{"Exception","Exception","Exception","Exception"} },
            };

        
        private static String[][] paramResults = {
            /*SMALLINT*/ {"1","1",null,"1"},
            /*INTEGER*/ {"1","1",null,"1"},
            /*BIGINT*/ {"1","1",null,"1"},
            /*DECIMAL(10,5)*/ {"1","1",null,"1"},
            /*REAL*/ {"1.0","1.0",null,"1.0"},
            /*DOUBLE*/ {"1.0","1.0",null,"1.0"},
            /*CHAR(60)*/ {"true","true","true","true"},
            /*VARCHAR(60)*/ {"true","true","true","true"},
            /*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},
            /*CHAR(60) FOR BIT DATA*/ {null,null,null,null},
            /*VARCHAR(60) FOR BIT DATA*/ {null,null,null,null},
            /*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},
            /*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},
            /*DATE*/ {"2000-01-01",null,null,"2000-01-01"},
            /*TIME*/ {"15:30:20",null,null,null},
            /*TIMESTAMP*/ {"2000-01-01 15:30:20.0",null,null,null},
            /*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}
        };

        // The client prints "1.00000" for DECIMAL(10,5) embedded just prints "1"
        private static String[][] paramResultsClient = {
            /*SMALLINT*/ {"1","1",null,"1"},
            /*INTEGER*/ {"1","1",null,"1"},
            /*BIGINT*/ {"1","1",null,"1"},
            /*DECIMAL(10,5)*/ {"1.00000","1.00000",null,"1.00000"},
            /*REAL*/ {"1.0","1.0",null,"1.0"},
            /*DOUBLE*/ {"1.0","1.0",null,"1.0"},
            /*CHAR(60)*/ {"true","true","true","true"},
            /*VARCHAR(60)*/ {"true","true","true","true"},
            /*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception"},
            /*CHAR(60) FOR BIT DATA*/ {null,null,null,null},
            /*VARCHAR(60) FOR BIT DATA*/ {null,null,null,null},
            /*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception"},
            /*CLOB(1k)*/ {"Exception","Exception","Exception","Exception"},
            /*DATE*/ {"2000-01-01",null,null,"2000-01-01"},
            /*TIME*/ {"15:30:20",null,null,null},
            /*TIMESTAMP*/ {"2000-01-01 15:30:20.0",null,null,null},
            /*BLOB(1k)*/ {"Exception","Exception","Exception","Exception"}
        };
      
    public NullIfTest(String name) {
        super(name);
        // TODO Auto-generated constructor stub
    }


    /**
     * Test NULLIF combinations on all datatypes
     * 
     * @throws SQLException
     */
    public void testAllDatatypesCombinations() throws SQLException {
        Statement s = createStatement();
        for (int firstColumnType = 0; firstColumnType < SQLUtilities.SQLTypes.length; firstColumnType++) {

            StringBuffer nullIfString = new StringBuffer("SELECT NULLIF("
                    + SQLUtilities.allDataTypesColumnNames[firstColumnType]);
            for (int secondColumnType = 0; secondColumnType < SQLUtilities.SQLTypes.length; secondColumnType++) {

                int row = 0;
                try {
                    StringBuffer completeNullIfString = new StringBuffer(
                            nullIfString.toString() + ","
                                    + SQLUtilities.allDataTypesColumnNames[secondColumnType]);
                    ResultSet rs = s.executeQuery(completeNullIfString
                            + ") from AllDataTypesTable");
                    while (rs.next()) {
                        String val = rs.getString(1);
                        if (usingDerbyNetClient())
                            assertEquals(
                                    nullIfResultsClient[firstColumnType][secondColumnType][row],
                                    val);
                        else
                            assertEquals(
                                    nullIfResults[firstColumnType][secondColumnType][row],
                                    val);
                        row++;
                    }
                    assertTrue(CastingTest.T_147b[firstColumnType][secondColumnType]);
                    rs.close();

                } catch (SQLException e) {
                    for (int r = row; r < 4; r++) {
                        if (usingDerbyNetClient())
                            assertEquals(
                                    nullIfResultsClient[firstColumnType][secondColumnType][row],
                                    "Exception");
                        else
                            assertEquals(
                                    nullIfResults[firstColumnType][secondColumnType][row],
                                    "Exception");

                    }
                    if (e.getSQLState().equals("42818"))
                        assertFalse(CastingTest.T_147b[firstColumnType][secondColumnType]);
                    else
                        assertEquals("22007", e.getSQLState());
                }
            }

        }
        s.close();
    }

    /**
     * Test NULLIF with parameter as first operand
     * 
     * @throws SQLException
     */
    public void testParameterForFirstOperandToNullIf() throws SQLException {
        for (int secondColumnType = 0; secondColumnType < SQLUtilities.SQLTypes.length; secondColumnType++) {

            String nullIfString = new String("SELECT NULLIF(?,"
                    + SQLUtilities.allDataTypesColumnNames[secondColumnType]
                    + ") from AllDataTypesTable");
            int row = 0;
            try {
                PreparedStatement ps = prepareStatement(nullIfString);
                switch (secondColumnType) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    ps.setBoolean(1, true);
                    break;
                case 8: // 'LONG VARCHAR'
                case 11: // 'LONG VARCHAR FOR BIT DATA'
                case 12: // 'CLOB'
                case 16: // 'BLOB'
                    // Take specific case of LONG VARCHAR. Prepare of
                    // nullif(?,long varchar)
                    // fails early on because at bind time, Derby tries to set ?
                    // to
                    // long varchar. But comparison between 2 long varchars is
                    // not
                    // supported and hence bind code in
                    // BinaryComparisonOperatorNode fails
                    // Similar thing happens for CLOB, BLOB and LONG VARCHAR FOR
                    // BIT DATA
                case 9:
                case 10:
                    ps.setBinaryStream(1, (java.io.InputStream) null, 1);
                    break;
                case 13:// DATE

                    ps.setDate(1, Date.valueOf("2000-01-01"));
                    break;
                case 14:// TIME

                    ps.setTime(1, Time.valueOf("15:30:20"));
                    break;
                case 15:// TIMESTAMP

                    ps
                            .setTimestamp(1, Timestamp
                                    .valueOf("2000-01-01 15:30:20"));
                    break;
                default:
                    break;
                }
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    String val = rs.getString(1);
                    
                    if (usingDerbyNetClient())
                        assertEquals(paramResultsClient[secondColumnType][row],
                                val);
                    else
                        assertEquals(paramResults[secondColumnType][row], val);
                    row++;
                }
                rs.close();
                ps.close();

            } catch (SQLException e) {
                for (int r = row; r < 4; r++) {
                    if (usingDerbyNetClient())
                        assertEquals(paramResultsClient[secondColumnType][row],
                                "Exception");
                    else
                        assertEquals(paramResults[secondColumnType][row],
                                "Exception");
                }
            }
        }
    }

    /**
     * Runs the test fixtures in embedded and client.
     * 
     * @return test suite
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("NullIfTest");

        suite.addTest(baseSuite("NullIfTest:embedded"));

        suite.addTest(TestConfiguration
                .clientServerDecorator(baseSuite("NullIfTest:client")));
        return suite;
    }

    public static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(name);
        suite.addTestSuite(NullIfTest.class);

        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the table used in the test cases.
             * 
             */
            protected void decorateSQL(Statement s) throws SQLException {
                SQLUtilities.createAndPopulateAllDataTypesTable(s);
            }

         
        };
    }

}
