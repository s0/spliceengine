package com.splicemachine.db.impl.sql.execute;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.ExceptionSeverity;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.dictionary.TriggerDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionStmtValidator;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.jdbc.Util;

/**
 * A trigger execution context (TEC) holds information that is available from the context of a trigger invocation.<br/>
 * A TEC is just a shell that gets pushed on a {@link TriggerExecutionStack} (initially empty). It is utilized by setting a
 * {@link TriggerDescriptor} on it and executing it's statement. After execution, the trigger descriptor is removed.<br/>
 * A TEC is removed from the stack when all of it's trigger executions, including any started by actions it may have
 * performed, have completed.
 * <p/>
 * A TEC will have to be serialized over synch boundaries so that trigger action information is available to
 * executions on other nodes.
 * <p/>
 * <h2>Possible Optimizations</h2>
 * <ul>
 * <li>We use a single row (for before/after row) implementation. This has changed from returning a heavy-weight
 * ResultSet to a lighter-weight ExecRow. We could support (for Statement Triggers) a batched up collection of
 * ExecRows.</li>
 * <li>A trigger rarely uses all columns in a row. We could use <code>changedColIds</code> to track only the required
 * new/old column values for the row.</li>
 * </ul>
 */
public class TriggerExecutionContext implements ExecutionStmtValidator, Externalizable {

    /* ========================
     * Serialized information
     * ========================
     */
    private int[] changedColIds;
    private String[] changedColNames;
    private String statementText;
    private UUID targetTableId;
    private String targetTableName;
    private ExecRow beforeResultSet;
    private ExecRow afterResultSet;
    private TriggerDescriptor triggerd;
    private ExecRow afterRow;   // used exclusively for InsertResultSets which have autoincrement columns.
    private TriggerEvent event;

    /* ========================
     * Not serialized
     * ========================
     */
    private boolean cleanupCalled;

    /*
     * Used to track all the result sets we have given out to users.  When the trigger context is no longer valid,
     * we close all the result sets that may be in the user space because they can no longer provide meaningful
     * results.
     */
    private List<ExecRow> resultSetList;

    /**
     * aiCounters is a list of AutoincrementCounters used to keep state which might be used by the trigger. This is
     * only used by Insert triggers--Delete and Update triggers do not use this variable.
     */
    private List<AutoincrementCounter> aiCounters;

    /*
     * aiHT is a map of autoincrement <key, value> pairs. This is used for ai values generated by the trigger.
     */
    private Map<String, Long> aiHT;

    /**
     * Build me a big old nasty trigger execution context. Damnit.
     *
     * @param statementText   the text of the statement that caused the trigger to fire.  may be null if we are replicating
     * @param changedColIds   the list of columns that changed.  Null for all columns or INSERT/DELETE.
     * @param changedColNames the names that correspond to changedColIds
     * @param targetTableId   the UUID of the table upon which the trigger fired
     * @param targetTableName the name of the table upon which the trigger fired
     * @param aiCounters      A list of AutoincrementCounters to keep state of the ai columns in this insert trigger.a
     */
    public TriggerExecutionContext(String statementText,
                                   int[] changedColIds,
                                   String[] changedColNames,
                                   UUID targetTableId,
                                   String targetTableName,
                                   List<AutoincrementCounter> aiCounters) throws StandardException {
        this();
        this.changedColIds = changedColIds;
        this.changedColNames = changedColNames;
        this.statementText = statementText;
        this.targetTableId = targetTableId;
        this.targetTableName = targetTableName;
        this.aiCounters = aiCounters;

        if (SanityManager.DEBUG) {
            if ((changedColIds == null) != (changedColNames == null)) {
                SanityManager.THROWASSERT("bad changed cols, " +
                        "(changedColsIds == null) = " + (changedColIds == null) +
                        "  (changedColsNames == null) = " + (changedColNames == null));
            }
            if (changedColIds != null) {
                SanityManager.ASSERT(changedColIds.length == (changedColNames != null ? changedColNames.length : 0),
                        "different number of changed col ids vs names");
            }
        }
    }

    public TriggerExecutionContext() {
        this.resultSetList = new ArrayList<>();
    }

    public void setBeforeResultSet(CursorResultSet rs) throws StandardException {
        if (rs == null) {
            return;
        }
        beforeResultSet = rs.getCurrentRow();
        try {
            rs.close();
        } catch (StandardException e) {
            // ignore - close quietly. We have only a single row impl currently
        }

    }

    public void setAfterResultSet(CursorResultSet rs) throws StandardException {
        if (rs == null) {
            return;
        }

        if (aiCounters != null) {
            if (triggerd.isRowTrigger()) {
                // An after row trigger needs to see the "first" row inserted
                rs.open();
                afterRow = rs.getNextRow();
                rs.close();
            } else {
                // after statement trigger needs to look at the last value.
                if (!triggerd.isBeforeTrigger()) {
                    resetAICounters(false);
                }
            }
        }
        afterResultSet = rs.getCurrentRow();
        try {
            rs.close();
        } catch (StandardException e) {
            // ignore - close quietly. We have only a single row impl currently
        }
    }

    public void setCurrentTriggerEvent(TriggerEvent event) {
        this.event = event;
    }

    public void clearCurrentTriggerEvent() {
        event = null;
    }

    public void setTrigger(TriggerDescriptor triggerd) {
        this.triggerd = triggerd;
    }

    public void clearTrigger() throws StandardException {
        event = null;
        triggerd = null;
        if (afterResultSet != null) {
            afterResultSet = null;
        }
        if (beforeResultSet != null) {
            beforeResultSet = null;
        }
        if (resultSetList != null && ! resultSetList.isEmpty()) {
            resultSetList.clear();
        }
    }

    /**
     * Cleanup the trigger execution context.  <B>MUST</B> be called when the caller is done with the trigger
     * execution context.
     * <p/>
     * We go to somewhat exaggerated lengths to free up all our resources here because a user may hold on
     * to a TEC after it is valid, so we clean everything up to be on the safe side.
     */
    public void cleanup() throws StandardException {
        if (!cleanupCalled) {

            resultSetList = null;

            if (afterResultSet != null) {
                afterResultSet = null;
            }
            if (beforeResultSet != null) {
                beforeResultSet = null;
            }
        }
        cleanupCalled = true;
    }

    /**
     * Make sure that the user isn't trying to get a result set after we have cleaned up.
     */
    private void ensureProperContext() throws SQLException {
        if (cleanupCalled) {
            throw new SQLException(
                    MessageService.getTextMessage(SQLState.LANG_STATEMENT_CLOSED_NO_REASON), "XCL31", ExceptionSeverity.STATEMENT_SEVERITY
            );
        }
    }

    /////////////////////////////////////////////////////////
    //
    // ExecutionStmtValidator
    //
    /////////////////////////////////////////////////////////

    /**
     * Make sure that whatever statement is about to be executed is ok from the context of this trigger.
     * <p/>
     * Note that we are sub classed in replication for checks for replication specific language.
     *
     * @param constantAction the constant action of the action that we are to validate
     */
    @Override
    public void validateStatement(ConstantAction constantAction) throws StandardException {

        // DDL statements are not allowed in triggers. Direct use of DDL
        // statements in a trigger's action statement is disallowed by the
        // parser. However, this runtime check is needed to prevent execution
        // of DDL statements by procedures within a trigger context.
        if (constantAction instanceof DDLConstantAction) {
            throw StandardException.newException(SQLState.LANG_NO_DDL_IN_TRIGGER, triggerd.getName(), constantAction.toString());
        }

        // No INSERT/UPDATE/DELETE for a before trigger. There is no need to
        // check this here because parser does not allow these DML statements
        // in a trigger's action statement in a before trigger. Parser also
        // disallows creation of before triggers calling procedures that modify
        // SQL data.

    }

    /////////////////////////////////////////////////////////
    //
    // TriggerExectionContext
    //
    /////////////////////////////////////////////////////////

    /**
     * Get the target table name upon which the trigger event is declared.
     *
     * @return the target table
     */
    public String getTargetTableName() {
        return targetTableName;
    }

    /**
     * Get the target table UUID upon which the trigger event is declared.
     *
     * @return the uuid of the target table
     */
    public UUID getTargetTableId() {
        return targetTableId;
    }


    /**
     * Get the text of the statement that caused the trigger to fire.
     *
     * @return the statement text
     */
    public String getEventStatementText() {
        return statementText;
    }

    /**
     * Get the columns that have been modified by the statement that caused this trigger to fire.  If all columns are
     * modified, will return null (e.g. for INSERT or DELETE will return null).
     *
     * @return an array of Strings
     */
    public String[] getModifiedColumns() {
        return changedColNames;
    }

    /**
     * Find out of a column was changed, by column name
     *
     * @param columnName the column to check
     * @return true if the column was modified by this statement.
     * Note that this will always return true for INSERT and DELETE regardless of the column name passed in.
     */
    public boolean wasColumnModified(String columnName) {
        if (changedColNames == null) {
            return true;
        }
        for (String changedColName : changedColNames) {
            if (changedColName.equals(columnName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Find out of a column was changed, by column number
     *
     * @param columnNumber the column to check
     * @return true if the column was modified by this statement.
     * Note that this will always return true for INSERT and DELETE regardless of the column name passed in.
     */
    public boolean wasColumnModified(int columnNumber) {
        if (changedColIds == null) {
            return true;
        }
        for (int i = 0; i < changedColNames.length; i++) {
            if (changedColIds[i] == columnNumber) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a result set row the old images of the changed rows. For a row trigger, the result set will have a
     * single row.  For a statement trigger, this result set has every row that has changed or will change.  If a
     * statement trigger does not affect a row, then the result set will be empty (i.e. ResultSet.next()
     * will return false).
     *
     * @return the ResultSet containing before images of the rows changed by the triggering event.
     * @throws SQLException if called after the triggering event has completed
     */
    private ExecRow getOldRowSet() throws SQLException {
        // private currently since no callers currently and we have impl of only 1 exec row at a time
        ensureProperContext();
        if (beforeResultSet == null) {
            return null;
        }
        if (this.event != null && this.event.isUpdate()) {
            return extractColumns(beforeResultSet, true);
        }
        return beforeResultSet;
    }

    /**
     * Returns a result set row the new images of the changed rows. For a row trigger, the result set will have a
     * single row.  For a statement trigger, this result set has every row that has changed or will change.  If a
     * statement trigger does not affect a row, then the result set will be empty (i.e. ResultSet.next()
     * will return false).
     *
     * @return the ResultSet containing after images of the rows changed by the triggering event.
     * @throws SQLException if called after the triggering event has completed
     */
    private ExecRow getNewRowSet() throws SQLException {
        // private currently since no callers currently and we have impl of only 1 exec row at a time
        ensureProperContext();
        if (afterResultSet == null) {
            return null;
        }
        if (this.event != null && this.event.isUpdate()) {
            return extractColumns(afterResultSet, false);
        }
        return afterResultSet;
    }

    /**
     * Like getBeforeResultSet(), but returns a result set positioned on the first row of the before result set.
     * Used as a convenience to get a column for a row trigger.  Equivalent to getBeforeResultSet() followed by next().
     *
     * @return the ResultSet positioned on the old row image.
     * @throws SQLException if called after the triggering event has completed
     */
    public ExecRow getOldRow() throws SQLException {
        return getOldRowSet();
    }

    /**
     * Like getAfterResultSet(), but returns a result set positioned on the first row of the before result set.
     * Used as a convenience to get a column for a row trigger.  Equivalent to getAfterResultSet() followed by next().
     *
     * @return the ResultSet positioned on the new row image.
     * @throws SQLException if called after the triggering event hascompleted
     */
    public ExecRow getNewRow() throws SQLException {
        return getNewRowSet();
    }

    public Long getAutoincrementValue(String identity) {
        // first search the map-- this represents the ai values generated by this trigger.
        if (aiHT != null) {
            Long value = aiHT.get(identity);
            if (value != null)
                return value;
        }

        // If we didn't find it in the map search in the counters which
        // represent values inherited by trigger from insert statements.
        if (aiCounters != null) {
            for (AutoincrementCounter aic : aiCounters) {
                if (identity.equals(aic.getIdentity())) {
                    return aic.getCurrentValue();
                }
            }
        }

        // didn't find it-- return NULL.
        return null;
    }

    /**
     * Copy a map of autoincrement values into the trigger execution context map of autoincrement values.
     */
    public void copyMapToAIHT(Map<String, Long> from) {
        if (from == null) {
            return;
        }
        if (aiHT == null) {
            aiHT = new HashMap<>();
        }
        aiHT.putAll(from);
    }

    /**
     * Reset Autoincrement counters to the beginning or the end.
     *
     * @param begin if True, reset the AutoincremnetCounter to the
     *              beginning-- used to reset the counters for the
     *              next trigger. If false, reset it to the end--
     *              this sets up the counter appropriately for a
     *              AFTER STATEMENT trigger.
     */
    public void resetAICounters(boolean begin) {
        if (aiCounters == null) {
            return;
        }

        afterRow = null;
        for (AutoincrementCounter aic : aiCounters) {
            aic.reset(begin);
        }
    }

    /**
     * Update Autoincrement Counters from the last row inserted.
     */
    public void updateAICounters() throws StandardException {
        if (aiCounters == null) {
            return;
        }
        for (AutoincrementCounter aic : aiCounters) {
            DataValueDescriptor dvd = afterRow.getColumn(aic.getColumnPosition());
            aic.update(dvd.getLong());
        }
    }

    @Override
    public String toString() {
        return "Name="+targetTableName+" triggerd=" + Objects.toString(triggerd);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        ArrayUtil.writeIntArray(out, changedColIds);
        ArrayUtil.writeArray(out, changedColNames);
        out.writeObject(statementText);
        out.writeObject(targetTableId);
        out.writeObject(targetTableName);
        out.writeObject(triggerd);
        out.writeObject(beforeResultSet);
        out.writeObject(afterResultSet);
        out.writeObject(afterRow);
        if (event != null) {
            out.writeBoolean(true);
            out.write(event.ordinal());
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        changedColIds = ArrayUtil.readIntArray(in);
        int len = ArrayUtil.readArrayLength(in);
        if (len != 0) {
            changedColNames = new String[len];
            ArrayUtil.readArrayItems(in, changedColNames);
        }
        statementText = (String) in.readObject();
        targetTableId = (UUID) in.readObject();
        targetTableName = (String) in.readObject();
        triggerd = (TriggerDescriptor) in.readObject();
        beforeResultSet = (ExecRow) in.readObject();
        afterResultSet = (ExecRow) in.readObject();
        afterRow = (ExecRow) in.readObject();
        if (in.readBoolean()) {
            event = TriggerEvent.values()[in.readInt()];
        }
    }

    private static ExecRow extractColumns(ExecRow resultSet, boolean firstHalf) throws SQLException {
        int nCols = (resultSet.nColumns() - 1) / 2;
        ExecRow result = new ValueRow(nCols);
        int sourceIndex = (firstHalf ? 1 : nCols + 1);
        int stopIndex = (firstHalf ? nCols : (resultSet.nColumns()-1));
        int targetIndex = 1;
        for (; sourceIndex<=stopIndex; sourceIndex++) {
            try {
                result.setColumn(targetIndex++, resultSet.getColumn(sourceIndex).cloneValue(false));
            } catch (StandardException e) {
                throw Util.generateCsSQLException(e);
            }
        }
        return result;
    }
}