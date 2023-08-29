package example.avro.binary;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class GenericBinConsumer {

    public static void main(String[] args) throws IOException {

        File file = new File("user_generic_bin.avro");

        Schema userSchema = new Schema.Parser().parse(new File("src/main/avro/User.avsc"));
        Schema newUserSchema = new Schema.Parser().parse(new File("src/main/avro/NewUser.avsc"));

        byte[] bytes = FileUtils.readFileToByteArray(file);

        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);

        // 1. specify writer schema
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(userSchema, newUserSchema);

        // 2. use same schema
//        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(userSchema);

        // 3. use different schema
//        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(newUserSchema);

        GenericRecord user = reader.read(null, decoder);

        System.out.println(user);
    }
}
