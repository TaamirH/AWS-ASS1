import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.sqs.model.Message;

public class ManagerProgram {
    private static final int MAX_INSTANCES = 10;
    private static final int THREAD_POOL_SIZE = 5; 
    private final AWS aws;
    private final String appToManagerQueueUrl;
    private final String managerToWorkerQueueUrl;
    private final String workerResultQueueUrl;
    private final String managerToAppQueueUrl;
    private final ExecutorService executorService;
    private final ConcurrentHashMap<String, Integer> queueFileCountMap;
    private final ConcurrentHashMap<String, BufferedWriter> appTagResultFiles;

    public ManagerProgram(String appToManagerQueueUrl,
                           String managerToAppQueueUrl) {
        this.aws = AWS.getInstance();
        this.appToManagerQueueUrl = appToManagerQueueUrl;
        this.managerToWorkerQueueUrl = aws.createQueue("ManagerToWorkerSQS");
        this.workerResultQueueUrl = aws.createQueue("WorkerToManagerSQS");
        this.managerToAppQueueUrl = managerToAppQueueUrl;
        this.executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE); // Adjust threads for message processing
        this.queueFileCountMap = new ConcurrentHashMap<>();
        this.appTagResultFiles = new ConcurrentHashMap<>();
    }

    public void run() {
        // Start a separate thread for processing worker results
        Thread resultProcessorThread = new Thread(() -> {
            try {
                processWorkerResults();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        resultProcessorThread.start();

        while (aws.isAcceptingNewMessages()) {
            try {
                // Poll for messages from AppToManagerSQS
                List<Message> messages = aws.receiveMessages(appToManagerQueueUrl);

                for (Message message : messages) {
                    System.out.println("Manager processing message: " + message.body());
                    if (message.body().contains("Terminate:True")) aws.stopAcceptingNewMessages();
                    executorService.submit(() -> processMessage(message));
                }

                Thread.sleep(2000); // Avoid excessive API calls
            } catch (Exception e) {
                System.err.println("Manager error: " + e.getMessage());
            }
        }
    }

    private static void bootstrapWorkers(AWS aws, String workerAmi, String workerTag,int n) throws InterruptedException {
        String queueUrl = aws.getQueueUrl("ManagerToWorkerSQS");
        int queueSize = aws.getQueueSize(queueUrl);
        int requiredWorkers = (int) Math.ceil((double) queueSize / n);
    
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

    private void processMessage(Message message) {
        try {
            String appTag = message.messageAttributes().get("AppTag").stringValue();
            System.out.println("Processing message with AppTag: " + appTag);

            // Parse S3 file URL from the message
            String[] parts = message.body().split(",");
            if (parts.length != 3){
                //return error to sqs
                return;
            }

            String s3FileUrl = parts[0].replace("File uploaded to S3:", "");
            String s3FileName = s3FileUrl.replace("s3://clilandtamil/", "");
            int n = Integer.valueOf(parts[1].replace("N:",""));

            // Download the input file
            File inputFile = new File(s3FileName);
            aws.downloadFileFromS3(s3FileName, inputFile);

            // Prepare result file for the appTag
            prepareResultFile(appTag);

            // Process the input file and update queueFileCountMap
            processInputFile(inputFile, appTag, n);

            // Delete the processed message from the queue
            aws.deleteMessageFromQueue(appToManagerQueueUrl, message);

        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
        }
    }

    private void processInputFile(File inputFile, String appTag, int n) {
        try {
            List<String> lines = Files.readAllLines(Paths.get(inputFile.toURI()));
            int lineCount = lines.size();
            bootstrapWorkers(aws, aws.IMAGE_AMI, "Worker", lineCount / n);
            queueFileCountMap.put(appTag, lineCount);

            for (String line : lines) {
                aws.sendMessageToQueue(managerToWorkerQueueUrl, line, appTag);
                System.out.println("Created task message: " + line);
            }
        } catch (Exception e) {
            System.err.println("Error reading input file: " + e.getMessage());
        }
    }

    private void prepareResultFile(String appTag) {
        String resultFileName = appTag;
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(resultFileName, true));
            appTagResultFiles.put(appTag, writer);
        } catch (IOException e) {
            System.err.println("Error preparing result file for AppTag " + appTag + ": " + e.getMessage());
        }
    }

    private void processWorkerResults() throws Exception {
        while (true) {
            try {
                List<Message> workerMessages = aws.receiveMessages(workerResultQueueUrl);

                for (Message workerMessage : workerMessages) {
                    String appTag = workerMessage.messageAttributes().get("AppTag").stringValue();

                    try {
                        BufferedWriter writer = appTagResultFiles.get(appTag);
                        if (writer != null) {
                            writer.write(workerMessage.body());
                            writer.newLine();
                            System.out.println("AppTag " + appTag + " result written to file.");
                        } else {
                            System.err.println("No result file found for AppTag " + appTag);
                        }

                        int remainingTasks = queueFileCountMap.computeIfPresent(appTag, (k, v) -> v - 1);

                        if (remainingTasks == 0) {
                            finalizeResults(appTag);
                        }

                        aws.deleteMessageFromQueue(workerResultQueueUrl, workerMessage);
                        if (!queueFileCountMap.keys().hasMoreElements()) handleTermination(aws, managerToWorkerQueueUrl, executorService);
                    } catch (IOException e) {
                        System.err.println("Error writing worker result: " + e.getMessage());
                    }
                }

                Thread.sleep(5000); // Polling delay
                
            } catch (InterruptedException e) {
                System.err.println("Error processing worker results: " + e.getMessage());
            }
        }
    }

    private void finalizeResults(String appTag) throws Exception {
        System.out.println("All tasks for AppTag " + appTag + " have been processed.");

        BufferedWriter writer = appTagResultFiles.remove(appTag);
        if (writer != null) {
            writer.close();
        }

        File resultFile = new File(appTag);
        String s3Key = "results/" + resultFile.getName();
        String s3Url = aws.uploadFileToS3(s3Key, resultFile);
        System.out.println("Result file uploaded to S3: " + s3Url);

        String completionMessage = "Results ready at: " + s3Url;
        aws.sendMessageToQueue(managerToAppQueueUrl, completionMessage, appTag);

        if (!resultFile.delete()) {
            System.err.println("Failed to delete local result file: " + resultFile.getName());
        }

        queueFileCountMap.remove(appTag);
    }

    public static void main(String[] args) {


        String appToManagerQueueUrl = "https://sqs.us-west-2.amazonaws.com/544427556982/AppToManagerSQS";
        String managerToAppQueueUrl = "https://sqs.us-west-2.amazonaws.com/544427556982/ManagerToAppSQS";

        ManagerProgram manager = new ManagerProgram(appToManagerQueueUrl, managerToAppQueueUrl);
        manager.run();
    }
}
