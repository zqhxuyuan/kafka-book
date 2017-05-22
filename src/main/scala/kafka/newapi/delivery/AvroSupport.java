package kafka.newapi.delivery;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Support class for avro messages.
 */
public class AvroSupport {


    public static Schema getSchema() {
        String schemaStr = "{\"namespace\": \"org.test.data\",\n" +
                "    \"type\": \"record\",\n" +
                "    \"name\": \"GSD\",\n" +
                "    \"fields\": [\n" +
                "        {\"name\": \"firstName\", \"type\": \"string\"}\n" +
                "    ]\n" +
                "}";
        return new Schema.Parser().parse(schemaStr);
    }

    public static byte[] dataToByteArray(Schema schema, GenericRecord datum) throws IOException {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            Encoder e = EncoderFactory.get().binaryEncoder(os, null);
            writer.write(datum, e);
            e.flush();
            byte[] byteData = os.toByteArray();
            return byteData;
        } finally {
            os.close();
        }
    }

    public static GenericRecord byteArrayToData(Schema schema, byte[] byteData) {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        ByteArrayInputStream byteArrayInputStream = null;
        try {
            byteArrayInputStream = new ByteArrayInputStream(byteData);
            Decoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);
            return reader.read(null, decoder);
        } catch (IOException e) {
            return null;
        } finally {
            try {
                byteArrayInputStream.close();
            } catch (IOException e) {

            }
        }
    }


    public static <T> T getValue(GenericRecord genericRecord, String name, Class<T> clazz) {
        Object obj = genericRecord.get(name);
        if (obj == null)
            return null;
        if (obj.getClass() == Utf8.class) {
            return (T) obj.toString();
        }
        if (obj.getClass() == Integer.class) {
            return (T) obj;
        }
        return null;
    }
}
