package example.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class SpecificBinConsumer {

    public static void main(String[] args) throws IOException {
        File file = new File("users_bin.avro");

        byte[] bytes = FileUtils.readFileToByteArray(file);

        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);

        SpecificDatumReader<NewUser> reader = new SpecificDatumReader<>(NewUser.class);

        NewUser user = reader.read(null, decoder);

        System.out.println(user);

    }
}
