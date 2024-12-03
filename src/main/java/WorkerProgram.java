import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

import software.amazon.awssdk.services.sqs.model.Message;

public class WorkerProgram {
    private final AWS aws;
    private final String managerToWorkerQueueUrl;
    private final String workerToManagerQueueUrl;

    public WorkerProgram(AWS aws, String managerToWorkerQueueUrl, String workerToManagerQueueUrl) {
        this.aws = aws;
        this.managerToWorkerQueueUrl = managerToWorkerQueueUrl;
        this.workerToManagerQueueUrl = workerToManagerQueueUrl;
    }

    public void run() {
        while (true) {
            List<Message> messages = aws.receiveMessages(managerToWorkerQueueUrl);

            for (Message message : messages) {
                String appTag = message.messageAttributes().get("AppTag").stringValue();
                try {
                    // Extract details from the message
                    String[] parts = parseInputLine(message.body());

                    if (parts.length != 2) {
                        throw new IllegalArgumentException("Invalid message format: " + message.body());
                    }
                    String operation = parts[0].trim(); // Operation (ToImage, ToHTML, ToText)
                    String fileUrl = parts[1].trim(); // S3 URL of the input PDF

                    // Download the PDF from S3
                    File pdfFile = downloadFileFromInternet(fileUrl);

                    // Perform the requested operation
                    File outputFile;
                    String outputKey;
                    String resultMessage;

                    try {
                        outputFile = performOperation(pdfFile, operation);
                        outputKey = "processed/" + outputFile.getName();
                        String outputS3Url = aws.uploadFileToS3(outputKey, outputFile);

                        // Success message
                        resultMessage = String.format(
                            "%s: to file: Original: %s, Output in s3: %s", operation, fileUrl, outputS3Url
                        );
                    } catch (Exception operationException) {
                        // Operation-specific error
                        resultMessage = String.format(
                            "%s: input file %s", operation, operationException.getMessage()
                        );
                    }

                    // Send a result message (success or error)
                    aws.sendMessageToQueue(workerToManagerQueueUrl, resultMessage, appTag);

                    // Remove the processed message from the queue
                    aws.deleteMessageFromQueue(managerToWorkerQueueUrl, message);

                } catch (Exception e) {
                    // General error handling (e.g., S3 file not available)
                    String errorMessage = String.format(
                        "Error: %s: input file %s", message.body(), e.getMessage()
                    );
                    aws.sendMessageToQueue(workerToManagerQueueUrl, errorMessage, appTag);

                    // Remove the processed message from the queue
                    aws.deleteMessageFromQueue(managerToWorkerQueueUrl, message);
                }
            }

            try {
                Thread.sleep(50); // Delay to avoid excessive API calls
            } catch (InterruptedException e) {
                System.err.println("Worker interrupted: " + e.getMessage());
                break;
            }
        }
    }

    private static File performOperation(File pdfFile, String operation) throws IOException {
        File outputFile;
        String name = pdfFile.getName();
        try (PDDocument document = PDDocument.load(pdfFile)) {
            switch (operation.toLowerCase()) {
                case "toimage":
                    outputFile = convertToImage(document, name);
                    break;

                case "tohtml":
                    outputFile = convertToHTML(document, name);
                    break;

                case "totext":
                    outputFile = convertToText(document, name);
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported operation: " + operation);
            }
        }

        return outputFile;
    }

    private static File convertToImage(PDDocument document, String name) throws IOException {
        PDFRenderer renderer = new PDFRenderer(document);
        BufferedImage image = renderer.renderImageWithDPI(0, 300);
        File imageFile = new File(name.replace(".pdf", ".png"));
        ImageIO.write(image, "png", imageFile);

        return imageFile;
    }

    private static File convertToHTML(PDDocument document, String name) throws IOException {
        String text = new PDFTextStripper().getText(document);
        String htmlContent = "<html><body>" + text.replace("\n", "<br>") + "</body></html>";

        File htmlFile = new File(name.replace(".pdf", ".html"));
        try (FileWriter writer = new FileWriter(htmlFile)) {
            writer.write(htmlContent);
        }

        return htmlFile;
    }

    private static File convertToText(PDDocument document, String name) throws IOException {
        String text = new PDFTextStripper().getText(document);

        File textFile = new File(name.replace(".pdf", ".txt"));
        try (FileWriter writer = new FileWriter(textFile)) {
            writer.write(text);
        }

        return textFile;
    }

    public File downloadFileFromInternet(String fileUrl) throws IOException {
        URL url = new URL(fileUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setInstanceFollowRedirects(false); // Disable auto-follow

        // Handle redirect
        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_MOVED_PERM || responseCode == HttpURLConnection.HTTP_MOVED_TEMP) {
            String newUrl = connection.getHeaderField("Location");
            System.out.println("Redirected to: " + newUrl);
            url = new URL(newUrl); // Update URL to the redirected location
            connection = (HttpURLConnection) url.openConnection();
        }

        // Extract the original file name from the URL
        String originalFileName = new File(url.getPath()).getName();
        File downloadedFile = new File(originalFileName);

        // Download the file
        try (InputStream inputStream = connection.getInputStream()) {
            Files.copy(inputStream, downloadedFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }

        System.out.println("File downloaded: " + downloadedFile.getAbsolutePath());
        return downloadedFile;
    }

    public static String[] parseInputLine(String inputLine) {
        // Trim and normalize whitespace (replace tabs, multiple spaces with a single space)
        String normalizedLine = inputLine.trim().replaceAll("\\s+", " ");
        String[] error = new String[0];
        // Split into parts (expecting exactly two parts: command and URL)
        String[] parts = normalizedLine.split(" ", 2); // Split into at most 2 parts

        // Ensure we have both parts
        if (parts.length == 2) {
            return parts;
        }

        return error; // Invalid input line
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java WorkerProgram managerToWorkerQueueUrl workerToManagerQueueUrl");
            return;
        }

        String managerToWorkerQueueUrl = args[0];
        String workerToManagerQueueUrl = args[1];

        AWS aws = AWS.getInstance(); // Ensure AWS instance is initialized
        WorkerProgram workerProgram = new WorkerProgram(aws, managerToWorkerQueueUrl, workerToManagerQueueUrl);
        workerProgram.run();
    }
}
