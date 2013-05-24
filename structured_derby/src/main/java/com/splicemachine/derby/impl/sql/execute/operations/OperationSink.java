package com.splicemachine.derby.impl.sql.execute.operations;

import com.gotometrics.orderly.StringRowKey;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.hbase.TableWriter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author Scott Fines
 * Created on: 5/13/13
 */
public class OperationSink {
    private static final Logger LOG = Logger.getLogger(OperationSink.class);

    public static interface Translator{
        @Nonnull List<Mutation> translate(@Nonnull ExecRow row,byte[] postfix) throws IOException;

        /**
         * @return true if mutations with the same row key should be pushed to the same location in TEMP
         */
        boolean mergeKeys();
    }

    private final TableWriter tableWriter;
    private final SpliceOperation operation;
    private final byte[] taskId;
    private final byte[] taskIdCol;

    private long rowCount = 0;
    private byte[] postfix;

    private OperationSink(byte[] taskIdCol,byte[] taskId,SpliceOperation operation,TableWriter tableWriter) {
        this.tableWriter = tableWriter;
        this.operation = operation;
        this.taskId = taskId;
        this.taskIdCol = taskIdCol;
    }

    public static OperationSink create(SpliceOperation operation, byte[] taskId) throws IOException {
        //TODO -sf- move this to a static initializer somewhere

        return new OperationSink(SpliceConstants.TASK_ID_COL,taskId,operation,SpliceDriver.driver().getTableWriter());
    }

    public static OperationSink create(SpliceOperation operation,
                                       TableWriter writer,byte[] taskId) throws IOException {
        //TODO -sf- move this to a static initializer somewhere

        return new OperationSink(SpliceConstants.TASK_ID_COL,taskId,operation,writer);
    }

    public TaskStats sink(byte[] destinationTable) throws IOException {
        TaskStats.SinkAccumulator stats = TaskStats.uniformAccumulator();
        stats.start();

        Translator translator = operation.getTranslator();

        boolean isTempTable = Arrays.equals(destinationTable,SpliceConstants.TEMP_TABLE_BYTES);
        CallBuffer<Mutation> writeBuffer;
        try{
            writeBuffer = tableWriter.writeBuffer(destinationTable);

            ExecRow row;
            boolean shouldMakUnique = !translator.mergeKeys();
            do{
//                debugFailIfDesired(writeBuffer);

                long start = System.nanoTime();
                row = operation.getNextRowCore();
                if(row==null) continue;

                stats.readAccumulator().tick(System.nanoTime()-start);

                start = System.nanoTime();
                byte[] postfix = getPostfix(shouldMakUnique);
                List<Mutation> mutations = translator.translate(row,postfix);


                for(Mutation mutation:mutations){
                    writeBuffer.add(mutation);
                }
                debugFailIfDesired(writeBuffer);

                stats.writeAccumulator().tick(System.nanoTime() - start);
            }while(row!=null);
            writeBuffer.flushBuffer();
            writeBuffer.close();
        } catch (Exception e) { //TODO -sf- deal with Primary Key and Unique Constraints here
            SpliceLogUtils.logAndThrow(LOG, Exceptions.getIOException(e));
        }
        return stats.finish();
    }

    private byte[] getPostfix(boolean shouldMakUnique) {
        if(taskId==null && shouldMakUnique)
            return SpliceUtils.getUniqueKey();
        else if(taskId==null)
            postfix = SpliceUtils.getUniqueKey();
        if(postfix == null){
            postfix = new byte[taskId.length+(shouldMakUnique?8:0)];
            System.arraycopy(taskId,0,postfix,0,taskId.length);
        }
        if(shouldMakUnique){
            rowCount++;
            System.arraycopy(Bytes.toBytes(rowCount),0,postfix,taskId.length,8);
        }
        return postfix;
    }

    private void debugFailIfDesired(CallBuffer<Mutation> writeBuffer) throws Exception {
    /*
     * For testing purposes, if the flag FAIL_TASKS_RANDOMLY is set, then randomly decide whether
     * or not to fail this task.
     */
        if(SpliceConstants.debugFailTasksRandomly){
            double shouldFail = Math.random();
            if(shouldFail<SpliceConstants.debugTaskFailureRate){
                //make sure that we flush the buffer occasionally
//                if(Math.random()<2*SpliceConstants.debugTaskFailureRate)
                    writeBuffer.flushBuffer();
                //wait for 1 second, then fail
                try {
                    Thread.sleep(1000l);
                } catch (InterruptedException e) {
                    //we were interrupted! sweet, fail early!
                    throw new IOException(e);
                }

                //now fail with a retryable exception
                throw new IOException("Random task failure as determined by debugFailTasksRandomly");
            }
        }
    }

}
