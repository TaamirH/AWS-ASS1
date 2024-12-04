import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.ResourceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TagSpecification;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;



public class AWS {

    public final String IMAGE_AMI = "ami-08902199a8aa0bc09";
    public Region region1 = Region.US_EAST_1;
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;
    private final String bucketName;
    private static AWS instance = null;
    private static boolean acceptNewMessages;


    public AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region1).build();
        bucketName = "clilandtamy";
        acceptNewMessages=true;
    }

    public static AWS getInstance() {
        if (instance == null) {
            return new AWS();
        }

        return instance;
    }
    public void stopAcceptingNewMessages() {
        acceptNewMessages = false;
    }
    
    public boolean isAcceptingNewMessages() {
        return acceptNewMessages;
    }


//////////////////////////////////////////  EC2

    public void runInstanceFromAMI(String ami) {
        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(InstanceType.T2_MICRO)
                .minCount(1)
                .maxCount(5) // todo decide what to put here
                .build();

        // Launch the instance
        try {
            ec2.runInstances(runInstancesRequest);
        } catch (Exception ignored) {
        }
    }
    
    public void runManagerFromAMI(String ami, String tagKey, String tagValue) {
        String instanceProfileName = "LabInstanceProfile"; // Replace with your IAM Instance Profile Name
        String s3JarPath = "s3://"+bucketName+"/manager.jar"; // Replace with your S3 JAR path
        String managerJarFileName = "manager.jar";
        String command = String.format(
            "#!/bin/bash\n" +
            "yum update -y\n" +
            "yum install -y java-11-amazon-corretto\n" +
            "aws s3 cp %s /home/ec2-user/%s\n" +
            "java -jar /home/ec2-user/%s",
            s3JarPath, managerJarFileName, managerJarFileName
        );
    
        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
            .imageId(ami)
            .instanceType(InstanceType.T2_MICRO)
            .iamInstanceProfile(builder -> builder.name(instanceProfileName))
            .minCount(1)
            .maxCount(1)
            .userData(Base64.getEncoder().encodeToString(command.getBytes())) // Provide the user data script
            .tagSpecifications(TagSpecification.builder()
                    .resourceType(ResourceType.INSTANCE)
                    .tags(Tag.builder()
                            .key(tagKey)
                            .value(tagValue)
                            .build())
                    .build())
            .build();
    
        // Launch the instance
        try {
            ec2.runInstances(runInstancesRequest);
            System.out.println("Launched Manager instance with tag: " + tagKey + "=" + tagValue);
        } catch (Exception e) {
            throw new RuntimeException("Failed to launch instance: " + e.getMessage());
        }
    }
    

    public void checkAndStartManager(String ami, String managerTagKey, String managerTagValue) {
        // Check for existing manager instances
        List<Instance> instances = getAllInstances();
        boolean managerExists = instances.stream().anyMatch(instance -> 
            instance.tags().stream().anyMatch(tag -> 
                tag.key().equals(managerTagKey) && tag.value().equals(managerTagValue)
            )
            && instance.state().name().equals(InstanceStateName.RUNNING)
        );
    
        if (!managerExists) {
            // Start a new Manager instance
            System.out.println("No Manager found. Starting a new Manager...");
            runManagerFromAMI(ami, managerTagKey, managerTagValue);
        } else {
            System.out.println("Manager is already running.");
        }
    }

    public RunInstancesResponse runInstanceFromAmiWithScript(String ami, InstanceType instanceType, int min, int max, String script) {
        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(instanceType)
                .minCount(min)
                .maxCount(max)
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                // @ADD security feratures
                
                .build();

        // Launch the instance
        try {
            ec2.runInstances(runInstancesRequest);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public void bootstrapWorkers(int numWorkers, String ami, String workerTag) {
        String instanceProfileName = "LabInstanceProfile"; // Replace with your IAM Instance Profile Name
        String s3JarPath = "s3://"+bucketName+"/worker.jar"; // Replace with your S3 JAR path
        String jarFileName = "worker.jar";
        String command = String.format(
            "#!/bin/bash\n" +
            "yum update -y\n" +
            "yum install -y java-11-amazon-corretto\n" +
            "aws s3 cp %s /home/ec2-user/%s\n" +
            "java -jar /home/ec2-user/%s",
            s3JarPath, jarFileName, jarFileName
        );
    
        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
            .imageId(ami)
            .instanceType(InstanceType.T2_MICRO)
            .iamInstanceProfile(builder -> builder.name(instanceProfileName)) // Attach IAM role
            .minCount(numWorkers)
            .maxCount(numWorkers)
            .userData(Base64.getEncoder().encodeToString(command.getBytes())) // Provide the user data script
            .tagSpecifications(TagSpecification.builder()
                    .resourceType(ResourceType.INSTANCE)
                    .tags(Tag.builder()
                            .key("Role")
                            .value(workerTag)
                            .build())
                    .build())
            .build();
    
        // Launch the worker instances
        try {
            ec2.runInstances(runInstancesRequest);
            System.out.println("Launched " + numWorkers + " worker instances with tag: " + workerTag);
        } catch (Exception e) {
            System.err.println("Failed to bootstrap workers: " + e.getMessage());
        }
    }

    public List<Instance> getAllInstances() {
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder().build();

        DescribeInstancesResponse describeInstancesResponse = null;
        try {
            describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);
        } catch (Exception ignored) {
        }

        return describeInstancesResponse.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();
    }

    public List<Instance> getAllInstancesWithLabel(Label label) throws InterruptedException {
        DescribeInstancesRequest describeInstancesRequest =
                DescribeInstancesRequest.builder()
                        .filters(Filter.builder()
                                .name("tag:Label")
                                .values(label.toString())
                                .build())
                        .build();

        DescribeInstancesResponse describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);

        return describeInstancesResponse.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();
    }

    public void terminateInstance(String instanceId) {
        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        // Terminate the instance
        try {
            ec2.terminateInstances(terminateRequest);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println("Terminated instance: " + instanceId);
    }


    ////////////////////////////// S3

    public String uploadFileToS3(String keyPath, File file) throws Exception {
        System.out.printf("Start upload: %s, to S3\n", file.getName());

        PutObjectRequest req =
                PutObjectRequest.builder()
                
                .bucket(bucketName)
                        .key(keyPath)
                        .build();

        s3.putObject(req, file.toPath()); // we don't need to check if the file exist already
        // Return the S3 path of the uploaded file
        return "s3://" + bucketName + "/" + keyPath;
    }

    public void downloadFileFromS3(String keyPath, File outputFile) throws FileNotFoundException, IOException {
        System.out.println("Start downloading file " + keyPath + " to " + outputFile.getPath());

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyPath)
                .build();

        try {
            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);
            byte[] data = objectBytes.asByteArray();

            // Write the data to a local file.
            OutputStream os = new FileOutputStream(outputFile);
            os.write(data);
            System.out.println("Successfully obtained bytes from an S3 object");
            os.close();
        } catch (Exception ignored) {
        }
    }

    public void createBucket(String bucketName) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucketName)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .build())
                .build());
        s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                .bucket(bucketName)
                .build());
    }

    public SdkIterable<S3Object> listObjectsInBucket(String bucketName) {
        // Build the list objects request
        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(1)
                .build();

        ListObjectsV2Iterable listRes = null;
        try {
            listRes = s3.listObjectsV2Paginator(listReq);
        } catch (Exception ignored) {
        }
        // Process response pages
        listRes.stream()
                .flatMap(r -> r.contents().stream())
                .forEach(content -> System.out.println(" Key: " + content.key() + " size = " + content.size()));

        return listRes.contents();
    }

    public void deleteEmptyBucket(String bucketName) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
        try {
            s3.deleteBucket(deleteBucketRequest);
        } catch (Exception ignored) {
        }
    }

    public void deleteAllObjectsFromBucket(String bucketName) {
        SdkIterable<S3Object> contents = listObjectsInBucket(bucketName);

        Collection<ObjectIdentifier> keys = contents.stream()
                .map(content ->
                        ObjectIdentifier.builder()
                                .key(content.key())
                                .build())
                .toList();

        Delete del = Delete.builder().objects(keys).build();

        DeleteObjectsRequest multiObjectDeleteRequest = DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(del)
                .build();

        try {
            s3.deleteObjects(multiObjectDeleteRequest);
        } catch (Exception ignored) {
        }
    }

    public void deleteBucket(String bucketName) {
        deleteAllObjectsFromBucket(bucketName);
        deleteEmptyBucket(bucketName);
    }

    //////////////////////////////////////////////SQS

    /**
     * @param queueName
     * @return queueUrl
     */
    public String createQueue(String queueName) {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        CreateQueueResponse create_result = null;
        try {
            create_result = sqs.createQueue(request);
        } catch (Exception ignored) {
        }

        assert create_result != null;
        String queueUrl = create_result.queueUrl();
        System.out.println("Created queue '" + queueName + "', queue URL: " + queueUrl);
        return queueUrl;
    }

    public void deleteQueue(String queueUrl) {
        DeleteQueueRequest req =
                DeleteQueueRequest.builder()
                        .queueUrl(queueUrl)
                        .build();

        try {
            sqs.deleteQueue(req);
        } catch (Exception ignored) {
        }
    }

    public String getQueueUrl(String queueName) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = null;
        try {
            queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        } catch (Exception ignored) {
        }
        System.out.println("Queue URL: " + queueUrl);
        return queueUrl;
    }

    public int getQueueSize(String queueUrl) {
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED
                )
                .build();

        GetQueueAttributesResponse queueAttributesResponse = null;
        try {
            queueAttributesResponse = sqs.getQueueAttributes(getQueueAttributesRequest);
        } catch (Exception ignored) {
        }
        Map<QueueAttributeName, String> attributes = queueAttributesResponse.attributes();

        return Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)) +
                Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)) +
                Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED));
    }

    public void sendMessageToQueue(String queueUrl, String messageBody, String appTag) {
        try {
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .messageAttributes(Map.of(
                        "AppTag", MessageAttributeValue.builder()
                                .stringValue(appTag)
                                .dataType("String")
                                .build()
                    ))
                    .build();
            sqs.sendMessage(sendMessageRequest);
            System.out.println("Message sent to queue: " + queueUrl + " | Message: " + messageBody + " | AppTag: " + appTag);
        } catch (Exception e) {
            System.out.println("Failed to send message to queue: " + e.getMessage());
        }
    }

    public List<Message> receiveMessages(String queueUrl) {
    try {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(5)
                .messageAttributeNames("All")
                .build();
        return sqs.receiveMessage(request).messages();
    } catch (Exception e) {
        System.out.println("Error receiving messages: " + e.getMessage());
        return List.of();
    }
}

public void deleteMessageFromQueue(String queueUrl, Message message) {
    try {
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(request);
        System.out.println("Deleted message: " + message.body());
    } catch (Exception e) {
        System.out.println("Error deleting message: " + e.getMessage());
    }
}


    ///////////////////////

    public enum Label {
        Manager,
        Worker
    }
}
