package example.avro.schema.registry;

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;

public class SchemaCompatibility {

    public static void main(String[] args) throws IOException {

        Schema schema = new Schema.Parser().parse(new File("src/main/avro/User.avsc"));
        Schema newSchema = new Schema.Parser().parse(new File("src/main/avro/NewUser.avsc"));

        System.out.println("Backward: " + AvroCompatibilityLevel.BACKWARD.compatibilityChecker.isCompatible(newSchema, schema));
        System.out.println("Forward: " + AvroCompatibilityLevel.FORWARD.compatibilityChecker.isCompatible(newSchema, schema));
        System.out.println("Full: " + AvroCompatibilityLevel.FULL.compatibilityChecker.isCompatible(newSchema, schema));
    }
}
