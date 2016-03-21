import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

/**
 * Created by adrian on 21/03/2016.
 */
public class DataReader {
    public static HashMap<String, String> readData() throws IOException {
        HashMap<String, String> input = new HashMap<>();

        Files.list(Paths.get(System.getProperty("user.dir"), "data")).forEach(filePath -> {
            if(Files.isRegularFile(filePath)) {
                try {
                    input.put(filePath.getFileName().toString(), new String(Files.readAllBytes(filePath)));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        return input;
    }
}
