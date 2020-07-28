package test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.BOMInputStream ;

public class SimpleGeonamesConverter {

	

	public static void main(String[] args) {
		File myFile = new File(args[0]);


		try {
//			System.out.println(SimpleFileReader.countLines(myFile));
//			SimpleFileReader.getFirstLines(myFile, 10);
			
			File fout = new File(FilenameUtils.getFullPath(myFile.getPath()) 
						+ FilenameUtils.getBaseName(myFile.getName())+".nt");
			SimpleGeonamesConverter.convertGeonames(myFile,fout);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}




	}

	public static void getFirstLines(File file, int k) throws IOException {

		String csvName = FilenameUtils.getBaseName(file.getName());

		File fout = new File(file.getAbsolutePath() + csvName+"_shortened.txt");
		FileOutputStream fos = new FileOutputStream(fout);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		int i = 0;



		// iterate through the file line by line
		try(BufferedReader br = new BufferedReader(new FileReader(file))) {
			for(String line; (line = br.readLine()) != null; ) {
				bw.write(line);
				bw.newLine();
				i++;
				if (i>k) {
					bw.close();
					return;	
				}
			}
			// line is not visible here.
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	public static void convertGeonames(File filein,File fileout) throws IOException {

		Charset charset = StandardCharsets.UTF_8;
		
	    BOMInputStream bom = new BOMInputStream(FileUtils.openInputStream(filein));  
	    BufferedReader br =new BufferedReader(new InputStreamReader(bom, charset));
        
        FileOutputStream fos = FileUtils.openOutputStream(fileout);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, charset));
	        
		
		//process initial two lines
		String rdfTag = new String("</rdf:RDF>");
		String firstline = br.readLine();
		if (firstline !=null){
			firstline = br.readLine();
			bw.write(firstline.substring(0, (firstline.length()-rdfTag.length())));
		}
		
		int i=2;
		try {
			for(String line; (line = br.readLine()) != null; ) {
				if(i%2!=0) {
					
//					bw.write(line.substring(445));
					bw.write(line.substring(444, (line.length()-rdfTag.length())));
					bw.newLine();
				}
				i++;
//				if (i>100) {
//					bw.write(rdfTag);
//					bw.close();
//					return;	
//				}
			}
			bw.write(rdfTag);
			bw.close();
			// line is not visible here.
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return;

		

	}
	
	public static int countLines(File file) throws IOException {


		int i = 0;

		// iterate through the file line by line
		try(BufferedReader br = new BufferedReader(new FileReader(file))) {
			for(String line; (line = br.readLine()) != null; ) {

				i++;

			}
			// line is not visible here.
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return i;

	}

}
