import java.io.File;

public class LocalApp {
    public static void main(String[] args) {
        AWS aws = AWS.getInstance();
        String appToManagerQueueUrl = aws.getQueueUrl("AppToManagerSQS");

        try {
            // Upload a file to S3
            String s3Url = aws.uploadFileToS3("inputfile2.txt", new File("C:\\New folder\\DPL\\ASS1\\AWS-Exp\\input-sample-2.txt"));

            // Send a message to Manager
            String taskMessage = "File uploaded to S3: " + s3Url;
            aws.sendMessageToQueue(appToManagerQueueUrl, taskMessage, "LocalApp1");

            System.out.println("Message sent to AppToManagerSQS: " + taskMessage);
        } catch (Exception e) {
            System.err.println("Error in LocalApp: " + e.getMessage());
        }
    }
}
