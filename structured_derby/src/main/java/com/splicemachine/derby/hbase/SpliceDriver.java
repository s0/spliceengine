package com.splicemachine.derby.hbase;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.constants.TransactionConstants;
import com.splicemachine.derby.logging.DerbyOutputLoggerWriter;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJobScheduler;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler;
import com.splicemachine.derby.impl.job.scheduler.SimpleThreadedTaskScheduler;
import com.splicemachine.derby.utils.ZkUtils;
import com.splicemachine.hbase.SpliceMetrics;
import com.splicemachine.hbase.TableWriter;
import com.splicemachine.hbase.TempCleaner;
import com.splicemachine.job.*;
import com.splicemachine.si.api.SIConstants;
import com.splicemachine.tools.EmbedConnectionMaker;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.sql.Connection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 *         Created on: 3/1/13
 */
public class SpliceDriver {
    private static final Logger LOG = Logger.getLogger(SpliceDriver.class);
    private final List<Service> services = new CopyOnWriteArrayList<Service>();
    private static final int DEFAULT_PORT = 1527;
    private static final String DEFAULT_SERVER_ADDRESS = "0.0.0.0";
    private static final int DEFAULT_MAX_CONCURRENT_TASKS = 10;



    public static enum State{
        NOT_STARTED,
        INITIALIZING,
        RUNNING,
        STARTUP_FAILED, SHUTDOWN
    }

    public static interface Service{

        boolean start();

        boolean shutdown();
    }


    private static final SpliceDriver INSTANCE = new SpliceDriver();

    private AtomicReference<State> stateHolder = new AtomicReference<State>(State.NOT_STARTED);

    private volatile Properties props = new Properties();

    private volatile NetworkServerControl server;

    private volatile TableWriter writerPool;
    private volatile CountDownLatch initalizationLatch = new CountDownLatch(1);

    private ExecutorService executor;
    private TaskScheduler threadTaskScheduler;
    private JobScheduler jobScheduler;
    private TaskMonitor taskMonitor;
    private TempCleaner tempCleaner;

    private SpliceDriver(){
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("splice-lifecycle-manager").build();
        executor = Executors.newSingleThreadExecutor(factory);


        //TODO -sf- create a separate pool for writing to TEMP
        try {
            writerPool = TableWriter.create(SpliceUtils.config);
            threadTaskScheduler = SimpleThreadedTaskScheduler.create(SpliceUtils.config);
            jobScheduler = new CoprocessorJobScheduler(ZkUtils.getZkManager(),SpliceUtils.config);
            taskMonitor = new ZkTaskMonitor(CoprocessorTaskScheduler.baseQueueNode,ZkUtils.getRecoverableZooKeeper());
            tempCleaner = new TempCleaner(SpliceUtils.config);
        } catch (Exception e) {
            throw new RuntimeException("Unable to boot Splice Driver",e);
        }
    }

    public ZkTaskMonitor getTaskMonitor() {
        return (ZkTaskMonitor)taskMonitor;
    }

    public TableWriter getTableWriter() {
        return writerPool;
    }

    public Properties getProperties() {
        return props;
    }

    public TempCleaner getTempCleaner() {
        return tempCleaner;
    }

    public <T extends Task> TaskScheduler<T> getTaskScheduler() {
        return (TaskScheduler<T>)threadTaskScheduler;
    }

    public TaskSchedulerManagement getTaskSchedulerManagement() {
        //this only works IF threadTaskScheduler implements the interface!
        return (TaskSchedulerManagement)threadTaskScheduler;
    }

    public <J extends CoprocessorJob> JobScheduler<J> getJobScheduler(){
        return (JobScheduler<J>)jobScheduler;
    }

    public JobSchedulerManagement getJobSchedulerManagement() {
        return (JobSchedulerManagement)jobScheduler;
    }

    public void registerService(Service service){
        this.services.add(service);
        //If the service is registered after we've successfully started up, let it know on the same thread.
        if(stateHolder.get()==State.RUNNING)
            service.start();
    }

    public void deregisterService(Service service){
        this.services.remove(service);
    }

    public static SpliceDriver driver(){
        return INSTANCE;
    }

    public State getCurrentState(){
        return stateHolder.get();
    }

    public void start(){
        if(stateHolder.compareAndSet(State.NOT_STARTED,State.INITIALIZING)){
            executor.submit(new Callable<Void>(){
                @Override
                public Void call() throws Exception {
                    SpliceLogUtils.info(LOG,"Booting the SpliceDriver");
                    writerPool.start();

                    //all we have to do is create it, it will register itself for us
                    SpliceMetrics metrics = new SpliceMetrics();

                    //register JMX items
                    registerJMX();
                    boolean setRunning = true;
                    setRunning = ensureHBaseTablesPresent();
                    if(!setRunning) {
                        abortStartup();
                        return null;
                    }
                    setRunning = bootDatabase();
                    if(!setRunning){
                        abortStartup();
                        return null;
                    }
                    setRunning = startServices();
                    if(!setRunning) {
                        abortStartup();
                        return null;
                    }
                    setRunning = startServer();
                    if(!setRunning) {
                        abortStartup();
                        return null;
                    } else
                        stateHolder.set(State.RUNNING);
                        initalizationLatch.countDown();
                    return null;
                }
            });
        }
    }

    private boolean bootDatabase() throws Exception {
        Connection connection = null;
        try{
        	EmbedConnectionMaker maker = new EmbedConnectionMaker();
        	connection = maker.createNew();
        	return true;
        }finally{
            if(connection!=null)
                connection.close();
        }
    }


    public void shutdown(){
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try{
                    SpliceLogUtils.info(LOG,"Shutting down connections");
                    if(server!=null) server.shutdown();

                    SpliceLogUtils.info(LOG,"Shutting down services");
                    for(Service service:services){
                        service.shutdown();
                    }

                    SpliceLogUtils.info(LOG,"Destroying internal Engine");
                    stateHolder.set(State.SHUTDOWN);
                }catch(Exception e){
                    SpliceLogUtils.error(LOG,
                            "Unable to shut down properly, this may affect the next time the service is started",e);
                }
                return null;
            }
        });
    }

/********************************************************************************************/
    /*private helper methods*/

    private void registerJMX()  {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try{

            //register TableWriter
            ObjectName writerName = new ObjectName("com.splicemachine.writer:type=WriterStatus");
            mbs.registerMBean(writerPool,writerName);

            //register TaskScheduler
            ObjectName taskSchedulerName = new ObjectName("com.splicemachine.job:type=TaskSchedulerManagement");
            mbs.registerMBean(threadTaskScheduler,taskSchedulerName);

            //register TaskMonitor
            ObjectName taskMonitorName = new ObjectName("com.splicemachine.job:type=TaskMonitor");
            mbs.registerMBean(taskMonitor,taskMonitorName);

            //register JobScheduler
            ObjectName jobSchedulerName = new ObjectName("com.splicemachine.job:type=JobSchedulerManagement");
            mbs.registerMBean(jobScheduler,jobSchedulerName);

        } catch (MalformedObjectNameException e) {
            //we want to log the message, but this shouldn't affect startup
            SpliceLogUtils.error(LOG,"Unable to register JMX entries",e);
        } catch (NotCompliantMBeanException e) {
            SpliceLogUtils.error(LOG, "Unable to register JMX entries", e);
        } catch (InstanceAlreadyExistsException e) {
            SpliceLogUtils.error(LOG, "Unable to register JMX entries", e);
        } catch (MBeanRegistrationException e) {
            SpliceLogUtils.error(LOG, "Unable to register JMX entries", e);
        }
    }

    private boolean ensureHBaseTablesPresent() {
        SpliceLogUtils.info(LOG, "Ensuring Required Hbase Tables are present");
        HBaseAdmin admin = null;
        try{
            admin = new HBaseAdmin(SpliceUtils.config);
            if(!admin.tableExists(HBaseConstants.TEMP_TABLE_BYTES)){
                HTableDescriptor td = SpliceUtils.generateDefaultDescriptor(HBaseConstants.TEMP_TABLE);
                admin.createTable(td);
                SpliceLogUtils.info(LOG, HBaseConstants.TEMP_TABLE+" created");
            }
            if (!admin.tableExists(TransactionConstants.TRANSACTION_TABLE_BYTES)) {
                HTableDescriptor desc = new HTableDescriptor(TransactionConstants.TRANSACTION_TABLE_BYTES);
                desc.addFamily(new HColumnDescriptor(HBaseConstants.DEFAULT_FAMILY.getBytes(),
                        Integer.MAX_VALUE,
                        admin.getConfiguration().get(HBaseConstants.TABLE_COMPRESSION, HBaseConstants.DEFAULT_COMPRESSION),
                        HBaseConstants.DEFAULT_IN_MEMORY,
                        HBaseConstants.DEFAULT_BLOCKCACHE,
                        Integer.MAX_VALUE,
                        HBaseConstants.DEFAULT_BLOOMFILTER));
                desc.addFamily(new HColumnDescriptor(HBaseConstants.DEFAULT_FAMILY));
                desc.addFamily(new HColumnDescriptor(SIConstants.SNAPSHOT_ISOLATION_CHILDREN_FAMILY));
                admin.createTable(desc);
            }
            return true;
        }catch(Exception e){
            SpliceLogUtils.error(LOG,"Unable to set up HBase Tables",e);
            return false;
        }finally{
            if(admin!=null){
                try{
                    admin.close();
                } catch (IOException e) {
                    SpliceLogUtils.error(LOG,"Unable to close Hbase admin, this could be symptomatic of a deeper problem",e);
                }
            }
        }
    }
     private boolean startServices() {
        try{
            SpliceLogUtils.info(LOG, "Splice Engine is Running, Enabling Services");
            boolean started=true;
            for(Service service:services){
                started = started &&service.start();
            }
            return started;
        }catch(Exception e){
            //just in case the outside services decide to blow up on me
            SpliceLogUtils.error(LOG,"Unable to start services, aborting startup",e);
            return false;
        }
    }

    private void abortStartup() {
        stateHolder.set(State.STARTUP_FAILED);
    }

    private boolean startServer() {
        SpliceLogUtils.info(LOG, "Services successfully started, enabling Connections");
        try{
            String bindAddress = SpliceUtils.config.get("splice.server.address",DEFAULT_SERVER_ADDRESS);
            int bindPort = SpliceUtils.config.getInt("splice.server.port", DEFAULT_PORT);
            server = new NetworkServerControl(InetAddress.getByName(bindAddress),bindPort);
            server.setLogConnections(true);
            server.start(new DerbyOutputLoggerWriter());
//            server.setTimeSlice(100);
//            server.setMaxThreads(1000);

            SpliceLogUtils.info(LOG,"Ready to accept connections");
            return true;
        }catch(Exception e){
            SpliceLogUtils.error(LOG,"Unable to start Client/Server Protocol",e);
            return false;
        }
    }
}
