package example.avro.binary;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.FileUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class GenericBinProducer {

    public static void main(String[] args) throws IOException {

        File file = new File("user_generic_bin.avro");

        Schema userSchema = new Schema.Parser().parse(new File("src/main/avro/User.avsc"));
        Schema newUserSchema = new Schema.Parser().parse(new File("src/main/avro/NewUser.avsc"));

        GenericRecord user = new GenericData.Record(userSchema);
        user.put("name", "Alyssa");
        user.put("favorite_number", 122);

        // throw errors when field not exist
        // user.put("xxx", "test");


        // Serialize
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(userSchema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(user, encoder);
        encoder.flush();
        // write bytes array to file
        FileUtils.writeByteArrayToFile(file, out.toByteArray());
        out.close();
    }
}
