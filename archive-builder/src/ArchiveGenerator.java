import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

public class ArchiveGenerator {

	private List<String> headers;
	private List<String> records;
	private final String inputFilePath;
	private final String outputDirectory;
	private final AtomicInteger COUNTER = new AtomicInteger(1);
	private final long SIZE_OF_LOG_FILE_IN_BYTES = 406847488L;

	public ArchiveGenerator(String inputFilePath, String outputDir) {
		this.inputFilePath = inputFilePath;
		headers = new ArrayList<>();
		records = new ArrayList<>();
		this.outputDirectory = outputDir;
	}

	public void readContent(int headerRows) {
		headers = new ArrayList<>();
		records = new ArrayList<>();

		try (Stream<String> stream = Files.lines(Paths.get(inputFilePath))) {

			stream.forEach(new Consumer<String>() {
				int counter = 0;

				@Override
				public void accept(String t) {
					if (counter < headerRows) {
						headers.add(t);
						counter++;
					} else {
						records.add(t);
					}
				}
			});

		} catch (IOException e) {
			System.err.println("Invalid Input File Path: "+inputFilePath);
		}
	}
	
	public void generateArchives(int noOfArchives) throws FileNotFoundException {
		String filePattern = outputDirectory + "/datageneratorfile%d.log";
		File file = new File(String.format(filePattern, COUNTER.getAndIncrement()));
		
		file.delete();
		long position = 0;

		StringBuffer head = new StringBuffer();

		headers.forEach(header -> {
			head.append(header);
			head.append("\n");
		});

		String header = head.toString();

		StringBuffer contentBuf = new StringBuffer();
		records.forEach(record -> {
			contentBuf.append(record);
			contentBuf.append("\n");
		});

		String content = contentBuf.toString();
		boolean headerWritten = false;

		try (FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();) {
			MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, 4096 * 16 * 16 * 16);
			do {
				buffer.clear();
				if (!headerWritten) {
					buffer.put(header.getBytes());
					headerWritten = true;
				}
				
				buffer.put(content.getBytes());
				position = buffer.position();
				fileChannel.write(buffer);
			} while (fileChannel.size() < SIZE_OF_LOG_FILE_IN_BYTES);
			
			System.out.println("Done Creating Log file..");
			String targetGz = file.getPath() + ".gz";
			
			archiveGZFile(file, targetGz);
			
			if(noOfArchives > 1) {
				
				ExecutorService svc = Executors.newFixedThreadPool(4);
				
				List<Callable<String>> tasks = new ArrayList<>();
				
				for(int i=2; i<=noOfArchives; i++) {
					
					Callable<String> task = new Callable<String>() {

						@Override
						public String call() throws Exception {
							File targetFile = new File(String.format(filePattern, COUNTER.getAndIncrement())+".gz");
							targetFile.delete();
							try (FileChannel src = new FileInputStream(new File(targetGz)).getChannel();
									FileChannel dest = new FileOutputStream(targetFile).getChannel();) {
								dest.transferFrom(src, 0, src.size());
								System.out.println("Created Archive: "+targetFile.getAbsolutePath());
							} catch (IOException e) {
								e.printStackTrace();
							}
							
							return targetFile.getAbsolutePath();
						}
					};
					tasks.add(task);
				}
				
				try {
					svc.invokeAll(tasks);
					svc.shutdown();
					svc.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		} catch (IOException e) {
			System.err.println("Invalid File Location : "+e.getMessage());
		}
	}

	
	
	private void archiveGZFile(File sourceFile, String outputFile) throws IOException {
		
		byte[] buffer = new byte[4096 * 16 * 16];
		GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(sourceFile.getPath() + ".gz"));
		
		OutputStream gzipout = new GZIPOutputStream(gos) {
			{
				def.setLevel(Deflater.DEFAULT_COMPRESSION);
			}
		};
		
		FileInputStream fis = new FileInputStream(sourceFile);
		int len;
		while ((len = fis.read(buffer)) > 0) {
			gzipout.write(buffer, 0, len);
		}
		fis.close();
		gos.finish();
		gos.close();
		
		System.out.println("Done creating Archive.");
	}
	
	

}
