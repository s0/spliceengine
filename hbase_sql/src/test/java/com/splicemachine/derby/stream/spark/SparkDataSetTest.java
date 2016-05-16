package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.AbstractDataSetTest;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.spark.SparkDataSet;

/**
 * Created by jleach on 4/15/15.
 */
public class SparkDataSetTest extends AbstractDataSetTest{

    public SparkDataSetTest() {
        super();
    }

    @Override
    protected DataSet<ExecRow> getTenRowsTwoDuplicateRecordsDataSet() {
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(tenRowsTwoDuplicateRecords));
    }

}