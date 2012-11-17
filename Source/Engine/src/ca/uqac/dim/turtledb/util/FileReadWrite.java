package ca.uqac.dim.turtledb.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;

/**
 * Utility class for file reading and writing.
 * @author sylvain
 *
 */
public class FileReadWrite
{
	/**
	 * Default file encoding
	 */
	public static String DEFAULT_ENCODING = "utf-8";
	
	/**
	 * Returns the content of a file into a string
	 * @param filename The file name to read from
	 * @param encoding The file encoding (optional, default "utf-8")
	 * @return The contents of the file
	 * @throws IOException If reading the file was impossible
	 */
	public static String getFileContents(String filename, String encoding) throws IOException
	{
		StringBuilder text = new StringBuilder();
		String NL = System.getProperty("line.separator");
		Scanner scanner = new Scanner(new FileInputStream(filename), encoding);
		try {
			while (scanner.hasNextLine()){
				text.append(scanner.nextLine() + NL);
			}
		}
		finally{
			scanner.close();
		}
		return text.toString();
	}
	
	public static String getFileContents(String filename) throws IOException
	{
		return getFileContents(filename, DEFAULT_ENCODING);
	}
}
