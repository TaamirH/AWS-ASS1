import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import software.amazon.awssdk.services.sqs.model.Message;

public class ManagerThread extends Thread {
    private final AWS aws;
    private final String appToManagerQueueUrl;
    private final String managerToWorkerQueueUrl;

    public ManagerThread(AWS aws, String appToManagerQueueUrl, String managerToWorkerQueueUrl) {
        this.aws = aws;
        this.appToManagerQueueUrl = appToManagerQueueUrl;
        this.managerToWorkerQueueUrl = managerToWorkerQueueUrl;
    }

    @Override
    public void run() {
        while (true) {
            try {
                // Poll for messages from AppToManagerSQS
                List<Message> messages = aws.receiveMessages(appToManagerQueueUrl);

                for (Message message : messages) {
                    // Parse and process the message
                    System.out.println("Manager processing message: " + message.body());
                    processMessage(message, aws, managerToWorkerQueueUrl, appToManagerQueueUrl);
                }

                Thread.sleep(2000); // Avoid excessive API calls
            } catch (Exception e) {
                System.err.println("Manager error: " + e.getMessage());
            }
        }
    }
     private static void processInputFile(File inputFile, AWS aws, String queueUrl, String appTag) {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                
                aws.sendMessageToQueue(queueUrl, line, appTag); 
    
                System.out.println("Created task message: " + line);
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
}

