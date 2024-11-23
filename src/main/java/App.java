import java.io.File;

public class App 
{
    
    public static void main( String[] args ) throws Exception
    {
        AWS aws =  AWS.getInstance();
        aws.checkAndStartManager("ami-08902199a8aa0bc09", "Role", "Manager");
        try{
        String url1 =aws.uploadFileToS3("inputfile1.txt",new File("C:\\New folder\\DPL\\ASS1\\AWS-Exp\\input-sample-1.txt"));
        String url2 =aws.uploadFileToS3("inputfile2.txt",new File("C:\\New folder\\DPL\\ASS1\\AWS-Exp\\input-sample-2.txt"));
        String aTomQURL=aws.createQueue("AppToManagerSQS");
        aws.sendMessageToQueue(aTomQURL, "File uploaded to S3:" +url1,"LocalApp1");
        aws.sendMessageToQueue(aTomQURL, "File uploaded to S3:" +url2,"LocalApp1");
            
    
    
    
    }
        catch(Exception e){
            System.out.println(e);
        }
        

        
    }
}
