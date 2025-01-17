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

package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * Interface implemented by the nodes created by a {@code NodeFactory}. Callers
 * of the various {@code NodeFactory.getNode()} methods will typically cast the
 * returned node to a more specific sub-type, as this interface only contains
 * the methods needed by {@code NodeFactory} to initialize the node.
 */
public interface Node {

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1) throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2) throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3) throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3,
              Object arg4, Object arg5, Object arg6)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4,
              Object arg5, Object arg6, Object arg7)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4,
              Object arg5, Object arg6, Object arg7, Object arg8)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
              Object arg6, Object arg7, Object arg8, Object arg9)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
              Object arg6, Object arg7, Object arg8, Object arg9, Object arg10)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
              Object arg6, Object arg7, Object arg8, Object arg9, Object arg10,
              Object arg11)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
              Object arg6, Object arg7, Object arg8, Object arg9, Object arg10,
              Object arg11, Object arg12)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
              Object arg6, Object arg7, Object arg8, Object arg9, Object arg10,
              Object arg11, Object arg12, Object arg13)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
              Object arg6, Object arg7, Object arg8, Object arg9, Object arg10,
              Object arg11, Object arg12, Object arg13, Object arg14)
            throws StandardException;

    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
              Object arg6, Object arg7, Object arg8, Object arg9, Object arg10,
              Object arg11, Object arg12, Object arg13, Object arg14, Object arg15,
              Object arg16, Object arg17)
            throws StandardException;
}
