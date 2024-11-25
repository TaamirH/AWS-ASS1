import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import software.amazon.awssdk.services.sqs.model.Message;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import javax.imageio.ImageIO;

public class Worker {

    private static final String OUTPUT_BUCKET = "processed-pdfs-bucket";

    public static void main(String[] args) {
        AWS aws = AWS.getInstance();
        String workerQueueUrl = aws.getQueueUrl("ManagerToWorkerSQS");
        String resultQueueUrl = aws.createQueue("WorkerToManagerSQS");

        while (true) {
            List<Message> messages = aws.receiveMessages(workerQueueUrl);

            for (Message message : messages) {
                try {
                    // Extract details from the message
                    String[] parts = message.body().split("\t");
                    if (parts.length != 2) {
                        throw new IllegalArgumentException("Invalid message format: " + message.body());
                    }
                    String operation = parts[0].trim(); // Operation (ToImage, ToHTML, ToText)
                    String s3Url = parts[1].trim(); // S3 URL of the input PDF

                    // Download the PDF from S3
                    String pdfKey = s3Url.replace("s3://" + OUTPUT_BUCKET + "/", "");
                    File pdfFile = new File("input.pdf");
                    aws.downloadFileFromS3(pdfKey, pdfFile);

                    // Perform the requested operation
                    File outputFile = null;
                    String outputKey = null;
                    String resultMessage;

                    try {
                        outputFile = performOperation(pdfFile, operation);
                        outputKey = "processed/" + outputFile.getName();
                        String outputS3Url = aws.uploadFileToS3(outputKey, outputFile);

                        // Success message
                        resultMessage = String.format(
                            "%s: Original: %s, Output: %s", operation, s3Url, outputS3Url
                        );
                    } catch (Exception operationException) {
                        // Operation-specific error
                        resultMessage = String.format(
                            "%s: input file %s", operation, operationException.getMessage()
                        );
                    }

                    // Send a result message (success or error)
                    aws.sendMessageToQueue(resultQueueUrl, resultMessage, "WorkerTag");

                    // Remove the processed message from the queue
                    aws.deleteMessageFromQueue(workerQueueUrl, message);

                } catch (Exception e) {
                    // General error handling (e.g., S3 file not available)
                    String errorMessage = String.format(
                        "Error: %s: input file %s", message.body(), e.getMessage()
                    );
                    aws.sendMessageToQueue(resultQueueUrl, errorMessage, "WorkerTag");
                }
            }

            // Sleep briefly before polling again to avoid excessive API calls
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                System.err.println("Worker interrupted: " + e.getMessage());
            }
        }
    }

    private static File performOperation(File pdfFile, String operation) throws IOException {
        File outputFile;

        try (PDDocument document = PDDocument.load(pdfFile)) {
            switch (operation.toLowerCase()) {
                case "toimage":
                    outputFile = convertToImage(document);
                    break;

                case "tohtml":
                    outputFile = convertToHTML(document);
                    break;

                case "totext":
                    outputFile = convertToText(document);
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported operation: " + operation);
            }
        }

        return outputFile;
    }

    private static File convertToImage(PDDocument document) throws IOException {
        PDFRenderer renderer = new PDFRenderer(document);
        BufferedImage image = renderer.renderImageWithDPI(0, 300); // Convert the first page at 300 DPI

        File imageFile = new File("output.png");
        ImageIO.write(image, "png", imageFile);

        return imageFile;
    }

    private static File convertToHTML(PDDocument document) throws IOException {
        String text = new PDFTextStripper().getText(document);
        String htmlContent = "<html><body>" + text.replace("\n", "<br>") + "</body></html>";

        File htmlFile = new File("output.html");
        try (FileWriter writer = new FileWriter(htmlFile)) {
            writer.write(htmlContent);
        }

        return htmlFile;
    }

    private static File convertToText(PDDocument document) throws IOException {
        String text = new PDFTextStripper().getText(document);

        File textFile = new File("output.txt");
        try (FileWriter writer = new FileWriter(textFile)) {
            writer.write(text);
        }

        return textFile;
    }
}
