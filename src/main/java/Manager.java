import software.amazon.awssdk.services.sqs.model.*;
import java.io.*;
import java.util.List;
public class Manager {
    public static void main(String[] args) {
        AWS aws = AWS.getInstance();

        // Get the AppToManager SQS queue URL
        String appToManagerQueueUrl = aws.getQueueUrl("AppToManagerSQS");
        String managerToWorkerQueueUrl = aws.createQueue("ManagerToWorkerSQS");

        while (true) {
            // Poll for new messages from the AppToManager queue
            List<Message> messages = aws.receiveMessages(appToManagerQueueUrl);

            for (Message message : messages) {
                try {
                    // Process each message
                    System.out.println("Received message: " + message.body());
                    
                    String appTag = message.messageAttributes().get("AppTag").stringValue();
                    // Parse S3 file URL from the message
                    String s3FileUrl = message.body().replace("File uploaded to S3: ", "");
                    
                    // Download the input file
                    File inputFile = new File("input.txt");
                    aws.downloadFileFromS3(s3FileUrl.replace("s3://clilandtamil/", ""), inputFile);

                    // Read and process the input file
                    processInputFile(inputFile, aws, managerToWorkerQueueUrl,appTag);

                    // Delete the processed message from the queue
                    aws.deleteMessageFromQueue(appToManagerQueueUrl, message);
                } catch (Exception e) {
                    System.err.println("Error processing message: " + e.getMessage());
                }
            }

            // Sleep for a short duration before polling again
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                System.err.println("Manager interrupted: " + e.getMessage());
            }
        }
    }

    private static void processInputFile(File inputFile, AWS aws, String queueUrl, String appTag) {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Each line contains a URL and operation
                String[] parts = line.split(",");
                if (parts.length != 2) continue;
    
                String url = parts[0].trim();
                String operation = parts[1].trim();
    
                // Create and send a task message to the worker queue
                String taskMessage = "URL: " + url + ", Operation: " + operation;
                aws.sendMessageToQueue(queueUrl, taskMessage, appTag); 
    
                System.out.println("Created task message: " + taskMessage);
            }
        } catch (IOException e) {
            System.err.println("Error reading input file: " + e.getMessage());
        }
    }
}
