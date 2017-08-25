/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.hbase;

import java.io.IOException;

import com.splicemachine.pipeline.InitializationCompleted;
import com.splicemachine.si.data.hbase.ZkUpgradeK2;
import com.splicemachine.timestamp.impl.TimestampOracle;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.timestamp.api.TimestampBlockManager;
import com.splicemachine.timestamp.hbase.ZkTimestampBlockManager;
import com.splicemachine.timestamp.impl.TimestampServer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.derby.lifecycle.EngineLifecycleService;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.lifecycle.MasterLifecycle;
import com.splicemachine.olap.OlapServer;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.timestamp.api.TimestampBlockManager;
import com.splicemachine.timestamp.hbase.ZkTimestampBlockManager;
import com.splicemachine.timestamp.impl.TimestampServer;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Responsible for actions (create system tables, restore tables) that should only happen on one node.
 */
public class SpliceMasterObserver extends BaseMasterObserver {

    private static final Logger LOG = Logger.getLogger(SpliceMasterObserver.class);

    public static final byte[] INIT_TABLE = Bytes.toBytes("SPLICE_INIT");

    private TimestampServer timestampServer;
    private DatabaseLifecycleManager manager;
    private OlapServer olapServer;

    @Override
    public void start(CoprocessorEnvironment ctx) throws IOException {
        LOG.info("Starting SpliceMasterObserver");

        LOG.info("Starting Timestamp Master Observer");

        ZooKeeperWatcher zkw = ((MasterCoprocessorEnvironment)ctx).getMasterServices().getZooKeeper();
        RecoverableZooKeeper rzk = zkw.getRecoverableZooKeeper();

        HBaseSIEnvironment env=HBaseSIEnvironment.loadEnvironment(new SystemClock(),rzk);
        SConfiguration configuration=env.configuration();

        String timestampReservedPath=configuration.getSpliceRootPath()+HConfiguration.MAX_RESERVED_TIMESTAMP_PATH;
        int timestampPort=configuration.getTimestampServerBindPort();
        int timestampBlockSize = configuration.getTimestampBlockSize();

        TimestampBlockManager tbm= new ZkTimestampBlockManager(rzk,timestampReservedPath);
        this.timestampServer =new TimestampServer(timestampPort,tbm,timestampBlockSize);

        this.timestampServer.startServer();

        // Check upgrade from 2.0
        ZkUpgradeK2 upgradeK2 = new ZkUpgradeK2(configuration.getSpliceRootPath());
        if (upgradeK2.upgrading()) {
            LOG.info("We are upgrading from K2");
            TimestampOracle to = TimestampOracle.getInstance(tbm,timestampBlockSize);
            long threshold = to.getNextTimestamp();
            LOG.info("Setting old transactions threshold to " + threshold);
            upgradeK2.upgrade(threshold);
            LOG.info("Upgrade complete");
        }
        env.txnStore().setOldTransactions(upgradeK2.getOldTransactions());

        int olapPort=configuration.getOlapServerBindPort();
        this.olapServer = new OlapServer(olapPort,env.systemClock());
        this.olapServer.startServer(configuration);

        /*
         * We create a new instance here rather than referring to the singleton because we have
         * a problem when booting the master and the region server in the same JVM; the singleton
         * then is unable to boot on the master side because the regionserver has already started it.
         *
         * Generally, this isn't a problem because the underlying singleton is constructed on demand, so we
         * will still only create a single manager per JVM in a production environment, and we avoid the deadlock
         * issue during testing
         */
        this.manager = new DatabaseLifecycleManager();
        super.start(ctx);
    }

    @Override
    public void stop(CoprocessorEnvironment ctx) throws IOException {
        LOG.warn("Stopping SpliceMasterObserver");
        manager.shutdown();
        this.timestampServer.stopServer();
    }

    @Override
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
        SpliceLogUtils.info(LOG, "preCreateTable %s", Bytes.toString(desc.getTableName().getName()));
        if (Bytes.equals(desc.getTableName().getName(), INIT_TABLE)) {
            switch(manager.getState()){
                case NOT_STARTED:
                    boot();
                case BOOTING_ENGINE:
                case BOOTING_GENERAL_SERVICES:
                case BOOTING_SERVER:
                    throw new PleaseHoldException("Please Hold - Starting");
                case RUNNING:
                    throw new InitializationCompleted("Success");
                case STARTUP_FAILED:
                case SHUTTING_DOWN:
                case SHUTDOWN:
                    throw new IllegalStateException("Startup failed");
            }
        }
    }

    @Override
    public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        boot();
    }

    private synchronized void boot() throws IOException{
        //make sure the SIDriver is booted
        if (! manager.getState().equals(DatabaseLifecycleManager.State.NOT_STARTED))
            return; // Race Condition, only load one...

        //make sure only one master boots at a time
        String lockPath = HConfiguration.getConfiguration().getSpliceRootPath()+HConfiguration.MASTER_INIT_PATH;
        SpliceMasterLock lock = new SpliceMasterLock(HConfiguration.getConfiguration().getSpliceRootPath(), lockPath, ZkUtils.getRecoverableZooKeeper());
        IOException exception = null;
        try {
            lock.acquire();

            //ensure that the SI environment is booted properly
            HBaseSIEnvironment env = HBaseSIEnvironment.loadEnvironment(new SystemClock(), ZkUtils.getRecoverableZooKeeper());
            SIDriver driver = env.getSIDriver();

            //make sure the configuration is correct
            SConfiguration config = driver.getConfiguration();

            //register the engine boot service
            try {
                MasterLifecycle distributedStartupSequence = new MasterLifecycle();
                manager.registerEngineService(new EngineLifecycleService(distributedStartupSequence, config, true));
                manager.start();
            } catch (Exception e1) {
                LOG.error("Unexpected exception registering boot service", e1);
                throw new DoNotRetryIOException(e1);
            }
        } catch (IOException e) {
            exception = e;
            throw exception;
        } catch (Exception e) {
            exception = new IOException("Error locking " + lockPath + " for master initialization", e);
            throw exception;
        } finally {
            if (lock.isAcquired()) {
                try {
                    lock.release();
                } catch (Exception e) {
                    if (exception != null)
                        throw exception;
                    else
                        throw new IOException("Error releasing " + lockPath + " after master initialization", e);
                }
            }
        }
    }

}
