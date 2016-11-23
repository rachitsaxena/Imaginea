import java.util.Scanner;

public class ArchiveBuilderClient {

	public static void main(String[] args) throws Exception {
		try(Scanner scanner = new Scanner(System.in);){
			System.out.println("Enter Input file path: ");
			String inputFilePath = scanner.nextLine();
			System.out.println("Enter Output directory path:");
			String outputDirectoryPath = scanner.nextLine();
			System.out.println("Enter number of archives required:");
			int noOfArchives =  0;
			try {
				noOfArchives = Integer.parseInt(scanner.nextLine());
				if(noOfArchives < 1) {
					System.err.println("Invalid Number of files entered. Must be greater then 1.");
				}
			}catch (NumberFormatException e) {
				System.err.println("Invalid number of files entered.");
			}
			
			long start = System.currentTimeMillis();
			
			ArchiveGenerator archGenerator = new ArchiveGenerator(inputFilePath, outputDirectoryPath);
			archGenerator.readContent(3);
			archGenerator.generateArchives(noOfArchives);
			System.out.println("Time taken (Secs): "+((System.currentTimeMillis() - start)/1000));
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
}
