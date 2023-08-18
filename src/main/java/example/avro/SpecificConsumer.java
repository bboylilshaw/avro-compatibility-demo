package example.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;

public class SpecificConsumer {

    public static void main(String[] args) throws IOException {
        File file = new File("users.avro");
        // Deserialize Users from disk
        DatumReader<NewUser> datumReader = new SpecificDatumReader<>(NewUser.class);
        DataFileReader<NewUser> dataFileReader = new DataFileReader<>(file, datumReader);
        NewUser user = null;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }
}
