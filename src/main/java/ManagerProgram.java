import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import software.amazon.awssdk.services.sqs.model.Message;

public class ManagerProgram {
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
        this.executorService = Executors.newFixedThreadPool(4); // Adjust threads for message processing
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

        while (true) {
            try {
                // Poll for messages from AppToManagerSQS
                List<Message> messages = aws.receiveMessages(appToManagerQueueUrl);

                for (Message message : messages) {
                    System.out.println("Manager processing message: " + message.body());
                    executorService.submit(() -> processMessage(message));
                }

                Thread.sleep(2000); // Avoid excessive API calls
            } catch (Exception e) {
                System.err.println("Manager error: " + e.getMessage());
            }
        }
    }

    private void processMessage(Message message) {
        try {
            String appTag = message.messageAttributes().get("AppTag").stringValue();
            System.out.println("Processing message with AppTag: " + appTag);

            // Parse S3 file URL from the message
            String s3FileUrl = message.body().replace("File uploaded to S3: ", "");
            String s3FileName = s3FileUrl.replace("s3://clilandtami/", "");

            // Download the input file
            File inputFile = new File(s3FileName);
            aws.downloadFileFromS3(s3FileName, inputFile);

            // Prepare result file for the appTag
            prepareResultFile(appTag);

            // Process the input file and update queueFileCountMap
            processInputFile(inputFile, appTag);

            // Delete the processed message from the queue
            aws.deleteMessageFromQueue(appToManagerQueueUrl, message);

        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
        }
    }

    private void processInputFile(File inputFile, String appTag) {
        try {
            List<String> lines = Files.readAllLines(Paths.get(inputFile.toURI()));
            int lineCount = lines.size();

            queueFileCountMap.put(appTag, lineCount);

            for (String line : lines) {
                aws.sendMessageToQueue(managerToWorkerQueueUrl, line, appTag);
                System.out.println("Created task message: " + line);
            }
        } catch (IOException e) {
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
        if (args.length < 2) {
            System.err.println("Usage: java ManagerProgram appToManagerQueueUrl managerToWorkerQueueUrl workerResultQueueUrl managerToAppQueueUrl");
            return;
        }

        String appToManagerQueueUrl = args[0];
        String managerToAppQueueUrl = args[1];

        ManagerProgram manager = new ManagerProgram(appToManagerQueueUrl, managerToAppQueueUrl);
        manager.run();
    }
}
