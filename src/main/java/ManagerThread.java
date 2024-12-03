import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import software.amazon.awssdk.services.sqs.model.Message;

public class ManagerThread extends Thread {
    private final AWS aws;
    private final String appToManagerQueueUrl;
    private final String managerToWorkerQueueUrl;
    private final String workerResultQueueUrl; // New SQS Queue for worker results
    private final String managerToAppqString;
    private final ExecutorService executorService;
    private final ConcurrentHashMap<String, Integer> queueFileCountMap;
    private final ConcurrentHashMap<String, BufferedWriter> appTagResultFiles;
    private final ConcurrentHashMap<String, String> appTag2QueueUrl;

    public ManagerThread(AWS aws, String appToManagerQueueUrl, String managerToWorkerQueueUrl, String workerResultQueueUrl, String managerToAppQueueUrl) {
        this.aws = aws;
        this.appToManagerQueueUrl = appToManagerQueueUrl;
        this.managerToWorkerQueueUrl = managerToWorkerQueueUrl;
        this.workerResultQueueUrl = workerResultQueueUrl;
        this.managerToAppqString = managerToAppQueueUrl;
        this.executorService = Executors.newFixedThreadPool(1); // Thread pool with 4 threads for message processing
        this.queueFileCountMap = new ConcurrentHashMap<>();
        this.appTagResultFiles = new ConcurrentHashMap<>();
        this.appTag2QueueUrl = new ConcurrentHashMap<>();
    }

    @Override
    public void run()  {
        // Start the result processor in a separate thread
        Thread resultProcessorThread = new Thread(() -> {
            try {
                processWorkerResults();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        resultProcessorThread.start();

        while (true) {
            try {
                // Poll for messages from AppToManagerSQS
                List<Message> messages = aws.receiveMessages(appToManagerQueueUrl);

                for (Message message : messages) {
                    // Parse and process the message
                    System.out.println("Manager processing message: " + message.body());
                    executorService.submit(() -> processMessage(message, aws, managerToWorkerQueueUrl, appToManagerQueueUrl));
                }

                Thread.sleep(2000); // Avoid excessive API calls
            } catch (Exception e) {
                System.err.println("Manager error: " + e.getMessage());
            }
        }
    }

    private void processMessage(Message message, AWS aws, String managerToWorkerQueueUrl, String appToManagerQueueUrl) {
        try {
            // Extract app tag
            String appTag = message.messageAttributes().get("AppTag").stringValue();
            System.out.println("Processing message with AppTag: " + appTag);

            // Parse S3 file URL from the message
            String s3FileUrl = message.body().replace("File uploaded to S3: ", "");
            String s3FileName = s3FileUrl.replace("s3://clilandtami/", "");
            // Download the input file
            File inputFile = new File(s3FileName);
            aws.downloadFileFromS3(s3FileName, inputFile);

            // Prepare result file for the appTag
            prepareResultFile(appTag, s3FileName);

            // Process the input file and update queueFileCountMap
            processInputFile(inputFile, aws, managerToWorkerQueueUrl, appTag);

            // Delete the processed message from the queue
            aws.deleteMessageFromQueue(appToManagerQueueUrl, message);

        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
        }
    }

    private void processInputFile(File inputFile, AWS aws, String queueUrl, String appTag) {
    try {
        // Read all lines from the file into a List
        List<String> lines = Files.readAllLines(Paths.get(inputFile.toURI()));
        int lineCount = lines.size();

        // Initialize the queue file count map and result file for this appTag
        queueFileCountMap.put(appTag, lineCount);

        // Send messages to the worker queue
        for (String line : lines) {
            aws.sendMessageToQueue(queueUrl, line, appTag); // Send message to worker queue
            System.out.println("Created task message: " + line);
        }

    } catch (IOException e) {
        System.err.println("Error reading input file: " + e.getMessage());
    }
}
    
    private void prepareResultFile(String appTag, String inputFileName) {
        String resultFileName = appTag;
        try {
            // Ensure that the result file is created for the appTag
           
            BufferedWriter writer = new BufferedWriter(new FileWriter(resultFileName, true));
            appTagResultFiles.put(resultFileName, writer);  // Store the writer for later use
        } catch (IOException e) {
            System.err.println("Error preparing result file for filename " + resultFileName + ": " + e.getMessage());
        }
    }
    
    // The single thread that processes messages from workers
private void processWorkerResults() throws Exception {
    while (true) {
        try {
            // Poll for worker results from the workerResultQueueUrl
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

                    // Decrease the task count
                    int remainingTasks = queueFileCountMap.computeIfPresent(appTag, (k, v) -> v - 1);

                    // If all tasks are processed for this appTag
                    if (remainingTasks == 0) {
                        System.out.println("All tasks for AppTag " + appTag + " have been processed.");

                        // Close and clean up the result file writer
                        writer.close();
                        appTagResultFiles.remove(appTag);

                        // Upload the result file to S3
                        File resultFile = new File(appTag);
                        String s3Key = "results/" + resultFile.getName();
                        String s3Url = aws.uploadFileToS3(s3Key, resultFile);
                        System.out.println("Result file uploaded to S3: " + s3Url);

                        // Notify the local app with the S3 file location
                        String completionMessage = "Results ready at: " + s3Key;
                        aws.sendMessageToQueue(managerToAppqString, completionMessage, appTag);
                        System.out.println("Completion message sent to local app for AppTag " + appTag);

                        // Optionally delete the result file after uploading
                        if (resultFile.delete()) {
                            System.out.println("Local result file deleted: " + resultFile.getName());
                        } else {
                            System.err.println("Failed to delete local result file: " + resultFile.getName());
                        }

                        // Remove the appTag from queueFileCountMap
                        queueFileCountMap.remove(appTag);
                        appTag2QueueUrl.remove(appTag);
                       
                    }

                    // Delete the processed message from the queue
                    aws.deleteMessageFromQueue(workerResultQueueUrl, workerMessage);

                } catch (IOException e) {
                    System.err.println("Error writing worker result: " + e.getMessage());
                }
            }

            Thread.sleep(5000); // Polling delay to avoid excessive API calls

        } catch (InterruptedException e) {
            System.err.println("Error processing worker results: " + e.getMessage());
        }
    }
}
}
