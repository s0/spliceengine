package com.splicemachine.derby.stream.spark;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.function.TableScanTupleFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkScanSetBuilder<V> extends TableScannerBuilder<V> {
    private final String tableName;
    private final SparkDataSetProcessor dsp;

    public SparkScanSetBuilder(SparkDataSetProcessor dsp,String tableName){
        this.tableName=tableName;
        this.dsp = dsp;
    }

    @Override
    public DataSet<V> buildDataSet() throws StandardException{
        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(SIConstants.config);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_INPUT_CONGLOMERATE, tableName);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_JDBC_STR, "jdbc:splice://localhost:${ij.connection.port}/splicedb;user=splice;password=admin");
        conf.set(MRConstants.ONE_SPLIT_PER_REGION, "true");
        try {
            conf.set(com.splicemachine.mrio.MRConstants.SPLICE_SCAN_INFO,getTableScannerBuilderBase64String());
        } catch (IOException ioe) {
            throw StandardException.unexpectedUserException(ioe);
        }
        JavaPairRDD<RowLocation, ExecRow> rawRDD = ctx.newAPIHadoopRDD(conf, SMInputFormat.class,
                RowLocation.class, ExecRow.class);

        Function f=new SparkSpliceFunctionWrapper<>(new TableScanTupleFunction<>(dsp.createOperationContext(activation)));
        return new SparkDataSet<>(rawRDD.map(f));
    }
}
