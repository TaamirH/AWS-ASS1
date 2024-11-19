import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class App 
{
    
    public static void main( String[] args ) throws Exception
    {
        AWS aws =  AWS.getInstance();
       
        try{
        String url =aws.uploadFileToS3("inputfile1.txt",new File("C:\\New folder\\DPL\\ASS1\\AWS-Exp\\input-sample-1.txt"));}
        catch(Exception e){
            System.out.println(e);
        }
    }
}
