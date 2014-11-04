package filesplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;



public class FileSplit {
	
	public static void main(String[] args) throws IOException{
		
		String inputFile = args[0];
		String outputDirectory = args[1];
		int linesPerFile = Integer.parseInt(args[2]);
		FileReader fr = new FileReader(inputFile);
		BufferedReader textReader = new BufferedReader(fr);
		String line;
		int fileCounter=1;
		int lineCounter=0;
		String currentFile = outputDirectory.concat("/file001.dat");
		PrintWriter writer = new PrintWriter(currentFile, "UTF-8");
		
		
		
		int lineCounter2=0;
		while ((line = textReader.readLine())!=null){
			lineCounter2++;
		}
		textReader.close();
		
		if ((lineCounter%linesPerFile)!=0){
			System.out.println("please define parameter p such that each data chunk will contain the same number of baskets");
			return;
		}
		
		linesPerFile = (int) lineCounter2/linesPerFile;
		
		fr = new FileReader(inputFile);
		textReader = new BufferedReader(fr);
		int lineCounter3=0;
		while ((line = textReader.readLine())!=null){
			
			writer.println(line);
			lineCounter++;
			lineCounter3++;
			if (lineCounter==linesPerFile){
				writer.close();
				fileCounter++;
				lineCounter=0;
				currentFile = "file";
				if (fileCounter<10){
					currentFile=outputDirectory.concat("/file");
					currentFile=currentFile.concat("00"+fileCounter+".dat");
				}
				else if (fileCounter<100){
					currentFile=outputDirectory.concat("/file");
					currentFile=currentFile.concat("0"+fileCounter+".dat");
				}
				else{
					currentFile=outputDirectory.concat("/file");
					currentFile=currentFile.concat(fileCounter+".dat");
				}
				
				if (lineCounter3!=lineCounter2)
				writer = new PrintWriter(currentFile, "UTF-8");
			}
			
		}
		writer.close();
		textReader.close();
		
	}
	
	
}
