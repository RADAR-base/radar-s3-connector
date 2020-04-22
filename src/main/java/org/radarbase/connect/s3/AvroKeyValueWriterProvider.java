package org.radarbase.connect.s3;

import static io.confluent.connect.avro.AvroData.ANYTHING_SCHEMA;
import static org.apache.avro.Schema.Type.NULL;

import java.io.IOException;
import java.util.Arrays;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroKeyValueWriterProvider implements RecordWriterProvider<S3SinkConnectorConfig> {
    private static final Logger log = LoggerFactory
            .getLogger(AvroKeyValueWriterProvider.class);
    private final S3Storage storage;
    private final AvroData avroData;

    AvroKeyValueWriterProvider(S3Storage storage, AvroData avroData) {
        this.storage = storage;
        this.avroData = avroData;
    }

    public String getExtension() {
        return ".avro";
    }

    public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
        return new AvroKeyValueWriter(conf, filename);
    }

    private String getSchemaName(Schema schema) {
        if (schema != null) {
            String schemaName = schema.name();
            if (schemaName != null) {
                return schemaName;
            } else {
                return schema.type().getName();
            }
        } else {
            return null;
        }
    }

    private org.apache.avro.Schema getSchema(AvroData avroData, Schema schema) {
        if (schema == null) {
            return org.apache.avro.Schema.createUnion(
                    Arrays.asList(org.apache.avro.Schema.create(NULL), ANYTHING_SCHEMA));
        } else {
            return avroData.fromConnectSchema(schema);
        }
    }

    private class AvroKeyValueWriter implements RecordWriter {
        private final DataFileWriter<Object> writer;
        private final String filename;
        private final S3SinkConnectorConfig config;
        private org.apache.avro.Schema combinedSchema;
        S3OutputStream s3out;

        private AvroKeyValueWriter(S3SinkConnectorConfig config, String filename) {
            this.filename = filename;
            this.config = config;
            writer = new DataFileWriter<>(new GenericDatumWriter<>());
            combinedSchema = null;
        }


        @Override
        public void write(SinkRecord record) {

            final Schema keySchema = record.keySchema();
            final Schema valueSchema = record.valueSchema();

            if (this.combinedSchema == null) {

                SchemaBuilder.RecordBuilder<org.apache.avro.Schema> builder = SchemaBuilder
                        .record(getSchemaName(keySchema) + "_" + getSchemaName(valueSchema))
                        .namespace("org.radarbase.kafka").doc("combined key-value record");

                this.combinedSchema = builder.fields().name("key").doc("Key of a Kafka SinkRecord")
                        .type(getSchema(avroData, keySchema)).noDefault().name("value")
                        .doc("Value of a Kafka SinkRecord").type(getSchema(avroData, valueSchema))
                        .noDefault().endRecord();

                try {
                    log.info("Opening record writer for: {}", filename);
                    this.s3out = storage.create(filename, true);
                    this.writer.setCodec(CodecFactory.fromString(config.getAvroCodec()));
                    this.writer.create(combinedSchema, this.s3out);
                } catch (IOException var5) {
                    throw new ConnectException(var5);
                }
            }

            log.trace("Sink record: {}", record);
            GenericRecord combinedRecord = new GenericData.Record(combinedSchema);
            write(combinedRecord, 0, keySchema, record.key());
            write(combinedRecord, 1, valueSchema, record.value());
            Object value = avroData.fromConnectData(valueSchema, record.value());
            try {
                if (value instanceof NonRecordContainer) {
                    value = ((NonRecordContainer) value).getValue();
                }

                if (value == null) {
                    if (config.nullValueBehavior().equalsIgnoreCase(
                            S3SinkConnectorConfig.BehaviorOnNullValues.IGNORE.toString())) {
                        log.debug(
                                "Null valued record cannot be written to output as Avro. Skipping. Record Key: {}",
                                record.key());
                    } else {
                        throw new ConnectException(
                                "Null valued records are not writeable with current behavior.on.null.values 'settings.");
                    }
                } else {
                    this.writer.append(combinedRecord);
                }
            } catch (IOException var4) {
                throw new ConnectException(var4);
            }
        }

        private void write(GenericRecord record, int index, Schema schema, Object data) {
            if (data == null) {
                return;
            }
            Object outputData = avroData.fromConnectData(schema, data);
            if (outputData instanceof NonRecordContainer) {
                outputData = ((NonRecordContainer) outputData).getValue();
            }
            record.put(index, outputData);
        }

        @Override
        public void close() {
            try {
                this.writer.close();
            } catch (IOException var2) {
                throw new ConnectException(var2);
            }
        }

        @Override
        public void commit() {
            try {
                this.writer.flush();
                this.s3out.commit();
                this.writer.close();
            } catch (IOException var2) {
                throw new ConnectException(var2);
            }
        }
    }
}

