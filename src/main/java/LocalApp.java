import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import software.amazon.awssdk.services.sqs.model.Message;

public class LocalApp {
    public static void main(String[] args) {
        AWS aws = AWS.getInstance();
        String appToManagerQueueUrl = aws.createQueue("AppToManagerSQS");
        String ManagerToAppQueueUrl = aws.createQueue("ManagerToAppSQS");
        aws.checkAndStartManager("ami-08902199a8aa0bc09", "Role", "Manager");
        try {
            // Upload a file to S3
            String inputFileName = "input-sample-3.txt";
            File inputFile = new File("C:\\New folder\\DPL\\ASS1\\AWS-Exp\\" + inputFileName);
            String s3Url = aws.uploadFileToS3(inputFileName, inputFile);

            // Send a message to Manager
            String appTag = "LocalApp1 " + inputFileName;
            String taskMessage = "File uploaded to S3:" + s3Url+"," + "N:100,Terminate:True";
            aws.sendMessageToQueue(appToManagerQueueUrl, taskMessage, appTag);

            System.out.println("Message sent to AppToManagerSQS: " + taskMessage);
            Thread.sleep(15000);
            // Wait for the result message
            System.out.println("Waiting for result message...");
            boolean gotMessage = false;
            while (!gotMessage) {
                List<Message> messages = aws.receiveMessages(ManagerToAppQueueUrl);
                if (!messages.isEmpty()) {
                    for (Message message : messages) {
                        String resultAppTag = message.messageAttributes().get("AppTag").stringValue();
                        if (resultAppTag.equals(appTag)) {
                            // Process the result message
                            String resultMessage = message.body();
                            System.out.println("Received result message: " + resultMessage);

                            // Extract S3 URL for the result file
                            String resultFileName = "results-"+inputFileName+"+txt";

                            // Download the result file
                            File resultFile = new File(resultFileName);
                            aws.downloadFileFromS3("results/LocalApp1 input-sample-3.txt", resultFile);

                            System.out.println("Result file downloaded: " + resultFile.getAbsolutePath());

                            // Generate HTML file from the result file
                            generateHtmlFile(resultFile, "output.html");

                            // Delete the processed message
                            aws.deleteMessageFromQueue(ManagerToAppQueueUrl, message);

                            System.out.println("HTML file created: output.html");
                            gotMessage = true;
                            return; // Exit the program after processing
                        }
                    }
                }
                Thread.sleep(2000); // Polling delay to avoid excessive API calls
            }

        } catch (Exception e) {
            System.err.println("Error in LocalApp: " + e.getMessage());
        }
    }

    private static void generateHtmlFile(File textFile, String outputHtmlFileName) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(textFile.toURI()));
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputHtmlFileName))) {
            writer.write("<html><head><title>Results</title></head><body>");
            writer.write("<h1>Result File Content</h1><pre>");
            for (String line : lines) {
                writer.write(line);
                writer.write("\n");
            }
            writer.write("</pre></body></html>");
        }
    }
}
