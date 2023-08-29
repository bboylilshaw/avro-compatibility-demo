package example.avro;

import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;

public class SchemaUtils {

    public static Schema userSchema() throws IOException {
        return new Schema.Parser().parse(new File("src/main/avro/User.avsc"));
    }

    public static Schema newUserSchema() throws IOException {
        return new Schema.Parser().parse(new File("src/main/avro/NewUser.avsc"));
    }
}
