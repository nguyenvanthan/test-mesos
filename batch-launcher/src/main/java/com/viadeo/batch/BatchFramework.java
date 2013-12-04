package com.viadeo.batch;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.apache.mesos.Protos.*;

public class BatchFramework implements Scheduler {



    private final ExecutorInfo executor;
    private final int totalTasks;
    private int launchedTasks = 0;
    private int finishedTasks = 0;



    public BatchFramework(ExecutorInfo executor) {
        this(executor, 5);
    }

    public BatchFramework(ExecutorInfo executor, int totalTasks) {
        this.executor = executor;
        this.totalTasks = totalTasks;
    }

    @Override
    public void registered(SchedulerDriver driver,
                           FrameworkID frameworkId,
                           MasterInfo masterInfo) {
        System.out.println("Registered! ID = " + frameworkId.getValue());
    }

    @Override
    public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {}

    @Override
    public void disconnected(SchedulerDriver driver) {}

    @Override
    public void resourceOffers(SchedulerDriver driver,
                               List<Offer> offers) {
        for (Offer offer : offers) {
            List<TaskInfo> tasks = new ArrayList<TaskInfo>();
            if (launchedTasks < totalTasks) {
                TaskID taskId = TaskID.newBuilder()
                        .setValue(Integer.toString(launchedTasks++)).build();

                System.out.println("Launching task " + taskId.getValue());

                TaskInfo task = TaskInfo.newBuilder()
                        .setName("task " + taskId.getValue())
                        .setTaskId(taskId)
                        .setSlaveId(offer.getSlaveId())
                        .addResources(Resource.newBuilder()
                                .setName("cpus")
                                .setType(Value.Type.SCALAR)
                                .setScalar(Value.Scalar.newBuilder().setValue(1)))
                        .addResources(Resource.newBuilder()
                                .setName("mem")
                                .setType(Value.Type.SCALAR)
                                .setScalar(Value.Scalar.newBuilder().setValue(128)))
                        .setExecutor(ExecutorInfo.newBuilder(executor))
                        .build();
                tasks.add(task);
            }
            Filters filters = Filters.newBuilder().setRefuseSeconds(1).build();
            driver.launchTasks(offer.getId(), tasks, filters);
        }
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, OfferID offerId) {}

    @Override
    public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
        System.out.println("Status update: task " + status.getTaskId().getValue() +
                " is in state " + status.getState());
        if (status.getState() == TaskState.TASK_FINISHED) {
            finishedTasks++;
            System.out.println("Finished tasks: " + finishedTasks);
            if (finishedTasks == totalTasks) {
                driver.stop();
            }
        }
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver,
                                 ExecutorID executorId,
                                 SlaveID slaveId,
                                 byte[] data) {}

    @Override
    public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {}

    @Override
    public void executorLost(SchedulerDriver driver,
                             ExecutorID executorId,
                             SlaveID slaveId,
                             int status) {}

    public void error(SchedulerDriver driver, String message) {
        System.out.println("Error: " + message);
    }



    private static void usage() {
        String name = BatchFramework.class.getName();
        System.err.println("Usage: " + name + " masterHost <totalTasks>");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            usage();
            System.exit(1);
        }

        String uri = "/home/kris/mesos/frameworks/batch.zip";
        String commandToExecute = "java -cp \"batch-launcher-1.0.jar:lib/*\" com.viadeo.batch.BatchExecutor";

        ExecutorInfo executor = ExecutorInfo.newBuilder()
                .setExecutorId(ExecutorID.newBuilder().setValue("default"))
                .setCommand(CommandInfo.newBuilder()
                    .addUris(CommandInfo.URI.newBuilder().setValue(uri))
                    .setValue(commandToExecute)
                )
                .setName("Test Executor (Java)")
                .setSource("java_test")
                .build();

        FrameworkInfo.Builder frameworkBuilder = FrameworkInfo.newBuilder()
                .setUser("") // Have Mesos fill in the current user.
                .setName("Test Framework (Java)");

        // TODO(vinod): Make checkpointing the default when it is default
        // on the slave.
        if (System.getenv("MESOS_CHECKPOINT") != null) {
            System.out.println("Enabling checkpoint for the framework");
            frameworkBuilder.setCheckpoint(true);
        }

        FrameworkInfo framework = frameworkBuilder.build();

        Scheduler scheduler = args.length == 1
                ? new BatchFramework(executor)
                : new BatchFramework(executor, Integer.parseInt(args[1]));

        MesosSchedulerDriver driver = null;
        driver = new MesosSchedulerDriver(scheduler, framework, args[0]);


        int status = driver.run() == Status.DRIVER_STOPPED ? 0 : 1;

        // Ensure that the driver process terminates.
        driver.stop();

        System.exit(status);
    }
}