public class LocalSimulation {
    public static void main(String[] args) {
        AWS aws = AWS.getInstance();
        String appToManagerQueueUrl = aws.createQueue("AppToManagerSQS");
        String managerToWorkerQueueUrl = aws.createQueue("ManagerToWorkerSQS");
        String workerToManagerQueueUrl = aws.createQueue("WorkerToManagerrSQS");
        String managerToAppQueueUrl = aws.createQueue("ManagerToAppSQS");
        // Start the Manager thread
        ManagerThread manager = new ManagerThread(aws, appToManagerQueueUrl, managerToWorkerQueueUrl, workerToManagerQueueUrl, managerToAppQueueUrl);
        manager.start();

        // Start multiple Worker threads
        for (int i = 0; i < 3; i++) { // Example: 3 workers
            WorkerThread worker = new WorkerThread(aws, managerToWorkerQueueUrl, workerToManagerQueueUrl);
            worker.start();
        }

        // Simulate LocalApp sending messages
        LocalApp.main(args);
    }
}
