package state;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class FileSource implements SourceFunction<String> {

  public String filePath;

  public FileSource(String filePath) {
    this.filePath = filePath;
  }

  private InputStream inputStream;
  private BufferedReader reader;
  private Random random = new Random();

  @Override
  public void run(SourceContext<String> sourceContext) throws Exception {

    reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
    String line = null;
    while ((line = reader.readLine()) != null) {
      TimeUnit.MILLISECONDS.sleep(random.nextInt(500));
      sourceContext.collect(line);
    }
    if (reader != null) {
      reader.close();
    }
    if (inputStream != null) {
      inputStream.close();
    }
  }

  @Override
  public void cancel() {
    try {
      if (reader != null) {
        reader.close();
      }
      if (inputStream != null) {
        inputStream.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
