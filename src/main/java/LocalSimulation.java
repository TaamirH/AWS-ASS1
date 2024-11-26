public class LocalSimulation {
    public static void main(String[] args) {
        AWS aws = AWS.getInstance();
        String appToManagerQueueUrl = aws.createQueue("AppToManagerSQS");
        String managerToWorkerQueueUrl = aws.createQueue("ManagerToWorkerSQS");

        // Start the Manager thread
        ManagerThread manager = new ManagerThread(aws, appToManagerQueueUrl, managerToWorkerQueueUrl);
        manager.start();

        // Start multiple Worker threads
        for (int i = 0; i < 1; i++) { // Example: 3 workers
            WorkerThread worker = new WorkerThread(aws, managerToWorkerQueueUrl);
            worker.start();
        }

        // Simulate LocalApp sending messages
        LocalApp.main(args);
    }
}
