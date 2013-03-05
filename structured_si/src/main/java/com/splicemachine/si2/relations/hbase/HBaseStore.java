package com.splicemachine.si2.relations.hbase;

import com.splicemachine.si2.relations.api.Relation;
import com.splicemachine.si2.relations.api.RelationReader;
import com.splicemachine.si2.relations.api.RelationWriter;
import com.splicemachine.si2.relations.api.RowLock;
import com.splicemachine.si2.relations.api.TupleGet;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class HBaseStore implements RelationReader, RelationWriter {
	private final HBaseTableSource tableSource;

	public HBaseStore(HBaseTableSource tableSource) {
		this.tableSource = tableSource;
	}

	@Override
	public Relation open(String relationIdentifier) {
		return new HBaseRelation(tableSource.getTable(relationIdentifier));
	}

	private List toList(Object item) {
		return Arrays.asList(item);
	}

	private Iterator getSingleRow(HTable table, Get get)
			throws IOException {
		return toList(table.get(get)).iterator();
	}

	private Iterator getManyRows(HTable table, Scan scan)
			throws IOException {
		final ResultScanner scanner = table.getScanner(scan);
		return scanner.iterator();
	}

	@Override
	public Iterator read(Relation relation, TupleGet get) {
		try {
			HTable table = ((HBaseRelation) relation).table;
			if (get instanceof HBaseGetTupleGet) {
				return getSingleRow(table, ((HBaseGetTupleGet) get).get);
			} else if (get instanceof HBaseScanTupleGet) {
				return getManyRows(table, ((HBaseScanTupleGet) get).scan);
			} else {
				throw new RuntimeException( "unknown get class " + get.getClass().getName() );
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close(Relation relation) {
		try {
			((HBaseRelation) relation).table.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

    @Override
    public void write(Relation relation, List tuples) {
        try {
            List<Put> puts = new ArrayList<Put>();
            for (Object p : tuples) {
                puts.add(((HBaseTuplePut) p).put);
            }
            ((HBaseRelation) relation).table.put(puts);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RowLock lockRow(Relation relation, Object row) {
        try {
            return new HBaseRowLock(((HBaseRelation) relation).table.lockRow((byte[]) row));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unLockRow(Relation relation, RowLock lock) {
        try {
            ((HBaseRelation) relation).table.unlockRow(((HBaseRowLock) lock).lock);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean checkAndPut(Relation relation, Object family, Object qualifier, Object value, Object tuple) {
        try {
            Put put = ((HBaseTuplePut) tuple).put;
            return ((HBaseRelation) relation).table.checkAndPut(put.getRow(), (byte[]) family, (byte[]) qualifier, (byte[]) value, put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
