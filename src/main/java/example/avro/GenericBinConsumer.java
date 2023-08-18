package example.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class GenericBinConsumer {

    public static void main(String[] args) throws IOException {
        File file = new File("users_bin.avro");

        byte[] bytes = FileUtils.readFileToByteArray(file);

        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);

        Schema schema = new Schema.Parser().parse(new File("src/main/avro/NewUser.avsc"));
        Schema writerSchema = new Schema.Parser().parse(new File("src/main/avro/User.avsc"));

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema, schema);

        GenericRecord user = reader.read(null, decoder);

        System.out.println(user);
    }
}
