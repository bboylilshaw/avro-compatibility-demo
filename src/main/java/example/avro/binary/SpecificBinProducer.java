package example.avro.binary;

import example.avro.User;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class SpecificBinProducer {
    public static void main(String[] args) throws IOException {

        File file = new File("user_specific_bin.avro");

        User user = new User();
        user.setName("Alyssa");
        user.setFavoriteNumber(123);


        // Serialize
        DatumWriter<User> writer = new SpecificDatumWriter<>(User.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(user, encoder);
        encoder.flush();
        // write bytes array to file
        FileUtils.writeByteArrayToFile(file, out.toByteArray());
        out.close();
    }
}
