import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.sqs.model.Message;
public class Manager {
    private static final int MAX_INSTANCES = 10;
    private static final int MESSAGES_PER_WORKER = 100;
    private static final int THREAD_POOL_SIZE = 5; 




    private static void bootstrapWorkers(AWS aws, String workerAmi, String workerTag) throws InterruptedException {
        String queueUrl = aws.getQueueUrl("ManagerToWorkerSQS");
        int queueSize = aws.getQueueSize(queueUrl);
        int requiredWorkers = (int) Math.ceil((double) queueSize / MESSAGES_PER_WORKER);
    
        // Get active worker instances
        List<Instance> workers = aws.getAllInstancesWithLabel(AWS.Label.Worker);
    
        int activeWorkers = (int) workers.stream()
            .filter(instance -> instance.state().name().equals(InstanceStateName.RUNNING))
            .count();
    
        // Determine how many workers to launch
        int workersToLaunch = Math.min(MAX_INSTANCES - activeWorkers, Math.max(0, requiredWorkers - activeWorkers));
    
        if (workersToLaunch > 0) {
            System.out.printf("Launching %d new workers...%n", workersToLaunch);
            aws.bootstrapWorkers(workersToLaunch, workerAmi, workerTag);
        } else {
            System.out.println("No additional workers needed.");
        }
    }

    
    private static void handleTermination(AWS aws, String managerToWorkerQueueUrl, ExecutorService threadPool) {
        System.out.println("Termination signal received. Shutting down...");

        // Stop accepting new messages
        aws.stopAcceptingNewMessages();

        try {
            // Wait for workers to finish processing the queue
            while (aws.getQueueSize(managerToWorkerQueueUrl) > 0) {
                System.out.println("Waiting for workers to finish...");
                Thread.sleep(5000);
            }

            // Shutdown thread pool
            threadPool.shutdown();
            threadPool.awaitTermination(60, TimeUnit.SECONDS);

            // Terminate all worker instances
            List<Instance> workers = aws.getAllInstancesWithLabel(AWS.Label.Worker);
            for (Instance worker : workers) {
                if (worker.state().name().equals(InstanceStateName.RUNNING)) {
                    System.out.println("Terminating worker instance: " + worker.instanceId());
                    aws.terminateInstance(worker.instanceId());
                }
            }

            // Terminate the Manager process
            List<Instance> manager = aws.getAllInstancesWithLabel(AWS.Label.Manager);
            aws.terminateInstance(manager.get(0).instanceId());
            System.out.println("Manager shutting down.");
            System.exit(0);

        } catch (Exception e) {
            System.err.println("Error during termination: " + e.getMessage());
        }
    }

    private static void processInputFile(File inputFile, AWS aws, String queueUrl, String appTag) {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Each line contains a URL and operation
                String[] parts = line.split(",");
                if (parts.length != 2) continue;
    
                String operation = parts[0].trim();
                String url = parts[1].trim();
    
                // Create and send a task message to the worker queue
                String taskMessage = "URL: " + url + ", Operation: " + operation;
                aws.sendMessageToQueue(queueUrl, taskMessage, appTag); 
    
                System.out.println("Created task message: " + taskMessage);
            }
        } catch (IOException e) {
            System.err.println("Error reading input file: " + e.getMessage());
        }
    }

    private static void processMessage(Message message, AWS aws, String managerToWorkerQueueUrl, String appToManagerQueueUrl) {
        try {
            // Extract app tag
            String appTag = message.messageAttributes().get("AppTag").stringValue();

            System.out.println("Processing message with AppTag: " + appTag);

            // Parse S3 file URL from the message
            String s3FileUrl = message.body().replace("File uploaded to S3: ", "");

            // Download the input file
            File inputFile = new File("input.txt");
            aws.downloadFileFromS3(s3FileUrl.replace("s3://clilandtamil/", ""), inputFile);

            // Process the input file
            processInputFile(inputFile, aws, managerToWorkerQueueUrl, appTag);

            // Delete the processed message from the queue
            aws.deleteMessageFromQueue(appToManagerQueueUrl, message);

        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        AWS aws = AWS.getInstance();
        

        // Get the AppToManager SQS queue URL
        String appToManagerQueueUrl = aws.getQueueUrl("AppToManagerSQS");
        String managerToWorkerQueueUrl = aws.createQueue("ManagerToWorkerSQS");
        ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        while (aws.isAcceptingNewMessages()) {
            // Poll for new messages from the AppToManager queue
            List<Message> messages = aws.receiveMessages(appToManagerQueueUrl);

            for (Message message : messages) {
                try {
                    if (message.body().equals("TERMINATE")) {
                        handleTermination(aws, managerToWorkerQueueUrl, threadPool);
                            return;
                    }
                    // Process each message
                    System.out.println("Received message: " + message.body());
                    threadPool.submit(() -> processMessage(message, aws, managerToWorkerQueueUrl, appToManagerQueueUrl));
                } catch (Exception e) {
                    System.err.println("Error processing message: " + e.getMessage());
                }
            }
            // Sleep for a short duration before polling again
            try {
                bootstrapWorkers(aws, "ami-08902199a8aa0bc09", "Worker");
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                System.err.println("Manager interrupted: " + e.getMessage());
            }
        }
    }


}
