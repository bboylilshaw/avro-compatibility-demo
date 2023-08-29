package example.avro.file;

import example.avro.NewUser;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class SpecificProducer {
    public static void main(String[] args) throws IOException {
        NewUser user1 = new NewUser();
        user1.setName("Alyssa");


//        user1.setFavoriteNumber(256);
        // Leave favorite color null

        // Alternate constructor
//        User user2 = new User("Ben", 7, "red");
//        NewUser user2 = new NewUser("Ben", new HashMap<>(), 7, "red");

        // Construct via builder
//        User user3 = User.newBuilder().setName("Charlie").setFavoriteColor("blue").setFavoriteNumber(null).build();
        NewUser user3 = NewUser.newBuilder().setName("Charlie").setFavoriteColor("blue").build();

        System.out.println(user1);
//        System.out.println(user2);
        System.out.println(user3);

        // Serialize user1, user2 and user3 to disk
        DatumWriter<NewUser> userDatumWriter = new SpecificDatumWriter<>(NewUser.class);
        DataFileWriter<NewUser> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), new File("users.avro"));
        dataFileWriter.append(user1);
//        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();
    }
}
