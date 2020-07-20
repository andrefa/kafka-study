package kafka.study;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertiesLoader {

	public static Properties fromFile(String fileName) {
		try {
			Properties prop = new Properties();
			prop.load(new FileInputStream("src/main/resources/"+ fileName));
			return prop;
		} catch (IOException e) {
			throw new IllegalArgumentException("File not found.", e);
		}
	}

}
