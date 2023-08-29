package example.avro.binary;

import example.avro.User;
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

public class SpecificToGenericBinProducer {
    public static void main(String[] args) throws IOException {
        User user = new User();
        user.setName("Alyssa");
        user.setFavoriteNumber(12);

        File file = new File("user_generic_bin1.avro");
        Schema schema = new Schema.Parser().parse(new File("src/main/avro/User.avsc"));
        Schema newUserSchema = new Schema.Parser().parse(new File("src/main/avro/NewUser.avsc"));

        GenericRecord genericUser = convertToGenericRecord(schema, user);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] serializedValue = null;
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        datumWriter.write(genericUser, encoder);
        encoder.flush();
        serializedValue = out.toByteArray();
        out.close();

        FileUtils.writeByteArrayToFile(file, serializedValue);
    }

    public static <T> GenericRecord convertToGenericRecord(Schema schema, T data) {
        return (GenericRecord) GenericData.get().deepCopy(schema, data);
    }
}
