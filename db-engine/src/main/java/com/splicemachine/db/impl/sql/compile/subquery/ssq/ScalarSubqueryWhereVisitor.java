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
package com.splicemachine.db.impl.sql.compile.subquery.ssq;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.ast.ColumnUtils;
import com.splicemachine.db.impl.ast.RSUtils;
import com.splicemachine.db.impl.sql.compile.*;
import org.spark_project.guava.collect.Lists;

import java.util.List;

import static org.spark_project.guava.collect.Iterables.filter;

/**
 * Created by yxia on 10/11/17.
 */
public class ScalarSubqueryWhereVisitor implements Visitor{
    private boolean foundUnsupported;
    private boolean foundEligibleCorrelation;
    private JBitSet innerTableSetFromOuterBlock;

    public ScalarSubqueryWhereVisitor(JBitSet innerSet) {
        foundUnsupported = false;
        foundEligibleCorrelation = false;
        innerTableSetFromOuterBlock = innerSet;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (node instanceof AndNode)
            return node;
        if (node instanceof BinaryRelationalOperatorNode || node instanceof BinaryListOperatorNode) {
            List<ColumnReference> columnReferences = RSUtils.collectNodes(node, ColumnReference.class);
            List<ColumnReference> referencesCorrelated = Lists.newArrayList(filter(columnReferences, new ColumnUtils.IsCorrelatedPredicate()));

            /* GOOD: Neither side had correlated predicates at any level. */
            if (referencesCorrelated.isEmpty()) {
                return node;
            }

            // check if SSQ is correlated with inner of an outer table in the outer query block
            for (ColumnReference columnReference: referencesCorrelated) {
                if (innerTableSetFromOuterBlock.get(columnReference.getTableNumber())) {
                    foundUnsupported = true;
                    return node;
                }
            }

            if (!referencesCorrelated.isEmpty())
                foundEligibleCorrelation = true;
        } else {
            /* Can be anything as long as it is not correlated. */
            foundUnsupported = ColumnUtils.isSubtreeCorrelated(node);
        }
        return node;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        // drill down AndNode
        return !(node instanceof AndNode);
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return foundUnsupported;
    }

    public boolean isFoundUnsupported() {
        return foundUnsupported;
    }

    public boolean isFoundEligibleCorrelation() {
        return foundEligibleCorrelation;
    }
}
