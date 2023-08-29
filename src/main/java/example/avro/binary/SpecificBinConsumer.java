package example.avro.binary;

import example.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class SpecificBinConsumer {

    public static void main(String[] args) throws IOException {

        File file = new File("user_specific_bin.avro");

        Schema userSchema = new Schema.Parser().parse(new File("src/main/avro/User.avsc"));
        Schema newUserSchema = new Schema.Parser().parse(new File("src/main/avro/NewUser.avsc"));

        byte[] bytes = FileUtils.readFileToByteArray(file);

        // 1. use same right schema, this is recommended
        SpecificDatumReader<User> reader = new SpecificDatumReader<>(userSchema);

        // 2. specify both writer and reader schema, ok if reader schema is compatible
//        SpecificDatumReader<User> reader = new SpecificDatumReader<>(userSchema, newUserSchema);

        // 3. use different schema, might throw error
//        SpecificDatumReader<User> reader = new SpecificDatumReader<>(newUserSchema);

        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);

        User user = reader.read(null, decoder);

        System.out.println(user);

    }
}
