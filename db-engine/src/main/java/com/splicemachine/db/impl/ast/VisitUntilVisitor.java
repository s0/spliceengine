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

package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import org.spark_project.guava.base.Predicate;

/**
 * Visitor that applies a visitor until a stopping point defined by a predicate. Parameters of the traversal
 * defined by the wrapped visitor.
 *
 * @author P Trolard
 *         Date: 30/10/2013
 */
public class VisitUntilVisitor implements Visitor {
    private boolean stop = false;
    final Visitor v;
    final Predicate<? super Visitable> pred;

    public VisitUntilVisitor(final Visitor v, final Predicate<? super Visitable> pred){
        this.v = v;
        this.pred = pred;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (pred.apply(node)){
            stop = true;
            return node;
        }
        return v.visit(node, parent);
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return v.visitChildrenFirst(node);
    }

    @Override
    public boolean stopTraversal() {
        return stop;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return v.skipChildren(node);
    }
}
