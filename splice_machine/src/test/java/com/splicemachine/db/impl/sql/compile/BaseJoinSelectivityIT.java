package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import java.sql.Connection;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 *
 *
 *
 */
public class BaseJoinSelectivityIT extends SpliceUnitTest {

    public static void createJoinDataSet(SpliceWatcher spliceClassWatcher, String schemaName) throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table ts_10_spk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1))")
                .withInsert("insert into ts_10_spk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(6, "6", "1995-01-01 23:03:20", false),
                        row(7, "7", "1995-01-01 23:03:20", false),
                        row(8, "8", "1995-01-01 23:03:20", false),
                        row(9, "9", "1995-01-01 23:03:20", false),
                        row(10, "10", "1995-01-01 23:03:20", false) )).create();

        new TableCreator(conn)
                .withCreate("create table ts_10_mpk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1,c2))")
                .withInsert("insert into ts_10_mpk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(6, "6", "1995-01-01 23:03:20", false),
                        row(7, "7", "1995-01-01 23:03:20", false),
                        row(8, "8", "1995-01-01 23:03:20", false),
                        row(9, "9", "1995-01-01 23:03:20", false),
                        row(10, "10", "1995-01-01 23:03:20", false) )).create();

        new TableCreator(conn)
                .withCreate("create table ts_10_npk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null)")
                .withInsert("insert into ts_10_npk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(6, "6", "1995-01-01 23:03:20", false),
                        row(7, "7", "1995-01-01 23:03:20", false),
                        row(8, "8", "1995-01-01 23:03:20", false),
                        row(9, "9", "1995-01-01 23:03:20", false),
                        row(10, "10", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table ts_5_spk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1))")
                .withInsert("insert into ts_5_spk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table ts_5_mpk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1,c2))")
                .withInsert("insert into ts_5_mpk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table ts_5_npk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null)")
                .withInsert("insert into ts_5_npk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();
        new TableCreator(conn)
                .withCreate("create table ts_3_spk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1))")
                .withInsert("insert into ts_3_spk values(?,?,?,?)")
                .withRows(rows(
                        row(5, "5", "1960-01-01 23:03:20", false),
                        row(6, "6", "1980-01-01 23:03:20", false),
                        row(7, "7", "1985-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table t1(i varchar(30))")
                .withIndex("create index t1i on t1(i)")
                .create();

        new TableCreator(conn)
                .withCreate("create table t2(j varchar(30))")
                .withInsert("insert into t2 values(?)")
                .withIndex("create index t2j on t2(j)")
                .withRows(rows(
                        row("1"),
                        row("2"),
                        row("3"),
                        row("4")
                )).create();

        for (int i = 0; i < 10; i++) {
            spliceClassWatcher.executeUpdate("insert into t2 select * from t2");
        }

        conn.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                schemaName));
        conn.commit();

    }

    protected double getTotalCost(String s) {
        double cost = 0.0d;
        String[] strings = s.split(",");
        for(String string : strings) {
            String[] s1 = string.split("=");
            if (s1[0].compareTo("totalCost") == 0) {
                cost = Double.parseDouble(s1[1]);
                break;
            }
        }
        return cost;
    }
}