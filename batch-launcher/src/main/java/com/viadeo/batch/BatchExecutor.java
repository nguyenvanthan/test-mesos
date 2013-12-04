package com.viadeo.batch;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;

import static org.apache.mesos.Protos.*;

public class BatchExecutor implements Executor {

    private String url; // url to launch a job by rest api

    @Override
    public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
        // not implemented
    }

    @Override
    public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
        // not implemented
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        // not implemented
    }

    @Override
    public void killTask(ExecutorDriver driver, TaskID taskId) {
        // not implemented
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        // not implemented
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        // not implemented
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        // not implemented
    }

    @Override
    public void launchTask(ExecutorDriver driver, TaskInfo task) {

        TaskStatus status = TaskStatus.newBuilder()
                .setTaskId(task.getTaskId())
                .setState(TaskState.TASK_RUNNING).build();

        driver.sendStatusUpdate(status);


        // TODO call rest api of spring batch admin
        System.out.println("coucou !!");

        System.out.println("Running task " + task.getTaskId());

        // This is where one would perform the requested task.

        status = TaskStatus.newBuilder()
                .setTaskId(task.getTaskId())
                .setState(TaskState.TASK_FINISHED).build();

        driver.sendStatusUpdate(status);
    }


    public static void main(String[] args) throws Exception {
        MesosExecutorDriver driver = new MesosExecutorDriver(new BatchExecutor());
        System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
    }

}
