import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;


public class DocCreator {

	public static void main(String[] args) throws IOException {
		String input,output;
		input = args[0];
		output = args[1] ;
		
		
		Map<Integer, String> hMap = new HashMap<Integer, String>();
		int count = 0;
		InputStream ipsV=new FileInputStream(input+"vocab.txt");	
		BufferedReader br1 = new BufferedReader(new InputStreamReader(ipsV ));
		String line1 = br1.readLine();
		while(line1 != null){
			count++;
			hMap.put(count,line1);
			line1= br1.readLine();
		}
		
		InputStream ipsD=new FileInputStream(input+"data.txt");
		BufferedReader br2 = new BufferedReader(new InputStreamReader(ipsD ));
		String line2 = br2.readLine();
		while(line2 != null){
			
			String inRow = line2.toString();
			String[] inArr = inRow.split(" ");
			int docID = Integer.parseInt(inArr[0]);
			int wordID = Integer.parseInt(inArr[1]);
			String wordCount = inArr[2];
			
			 File f = new File(output + "//" + docID+".txt");
			 
			  if(f.exists()){
				  	FileWriter fileWritter = new FileWriter(f,true);
				    BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
				    
				  	for (int i = 1; i < wordID; i++) {
				  		bufferWritter.write(" ");
	    	        	bufferWritter.write("0");
					}
	    	      
	    	        	bufferWritter.write(" ");
	    	        	bufferWritter.write(wordCount);
	    	        
	    	        bufferWritter.close();
			  }else{
				  f.createNewFile();
				  PrintWriter writer = new PrintWriter(f, "UTF-8");
					for (int i = 1; i < wordID; i++) {
						if(i==1){
							writer.print("0");
						}else{
							writer.print(" ");
							writer.print("0");
						}
						
					}
					  writer.print(" ");
					  writer.print(wordCount);
					  //writer.print(wordID +" " + wordCount);
				//}
				  writer.close();
			  }
			
			  line2 = br2.readLine();
		}
	
	}
}







/*

String input,output;
input = args[0];
output = args[1] ;


Map<Integer, String> hMap = new HashMap<Integer, String>();
int count = 0;
InputStream ipsV=new FileInputStream(input+"vocab.txt");	
BufferedReader br1 = new BufferedReader(new InputStreamReader(ipsV ));
String line1 = br1.readLine();
while(line1 != null){
	count++;
	hMap.put(count,line1);
	line1= br1.readLine();
}

InputStream ipsD=new FileInputStream(input+"data.txt");
BufferedReader br2 = new BufferedReader(new InputStreamReader(ipsD ));
String line2 = br2.readLine();
while(line2 != null){
	
	String inRow = line2.toString();
	String[] inArr = inRow.split(" ");
	int docID = Integer.parseInt(inArr[0]);
	int wordID = Integer.parseInt(inArr[1]);
	int wordCount = Integer.parseInt(inArr[2]);
	
	 File f = new File(output + "//" + docID+".txt");
	 
	  if(f.exists()){
		  	FileWriter fileWritter = new FileWriter(f,true);
	        BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
	       // for (int i = 0; i < wordCount; i++) {
	        	bufferWritter.write("\n");
	        	bufferWritter.write(hMap.get(wordID)+" " + wordCount);
	        	//bufferWritter.write(wordID +" " + wordCount);
			//}
	        
	        bufferWritter.close();
	  }else{
		  f.createNewFile();
		  PrintWriter writer = new PrintWriter(f, "UTF-8");
		  //for (int i = 0; i < wordCount; i++) {
			  writer.print(hMap.get(wordID) +" " + wordCount);
			  //writer.print(wordID +" " + wordCount);
		//}
		  writer.close();
	  }
	
	  line2 = br2.readLine();
}
*/