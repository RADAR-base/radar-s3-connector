///*
// * Copyright 2017 Kings College London and The Hyve
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.radarbase.connect.s3;
//
//import static io.confluent.connect.avro.AvroData.ANYTHING_SCHEMA;
//import static org.apache.avro.Schema.Type.NULL;
//
//import java.io.IOException;
//import java.util.Arrays;
//
//import io.confluent.connect.avro.AvroData;
//import io.confluent.connect.storage.StorageSinkConnectorConfig;
//import io.confluent.connect.storage.format.Format;
//import io.confluent.connect.storage.format.RecordWriter;
//import io.confluent.connect.storage.format.RecordWriterProvider;
//import io.confluent.kafka.serializers.NonRecordContainer;
//import org.apache.avro.SchemaBuilder;
//import org.apache.avro.file.CodecFactory;
//import org.apache.avro.file.DataFileWriter;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericDatumWriter;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.Path;
//import org.apache.kafka.connect.data.Schema;
//import org.apache.kafka.connect.errors.ConnectException;
//import org.apache.kafka.connect.errors.DataException;
//import org.apache.kafka.connect.sink.SinkRecord;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * Writes data to HDFS using the Confluent Kafka HDFS connector.
// *
// * <p>This will write both the keys and values of the data to file, in a new Avro format with as
// * its two fields the "key" and "value" fields, with the original Avro schema of the key and value
// * as the type of those fields.
// *
// * <p>To use, implement a {@link Format#getRecordWriterProvider()} that returns this class (i.e.,
// * {@link AvroFormatRadar}), and provide that in the `format.class` property.
// */
//public class AvroKeyValueWriterProvider implements RecordWriterProvider<StorageSinkConnectorConfig> {
//
//    private static final Logger logger = LoggerFactory.getLogger(
//            AvroKeyValueWriterProvider.class);
//    private final AvroData avroData;
//    private static final String EXTENSION = ".avro";
//
//    public AvroKeyValueWriterProvider(AvroData avroData) {
//        this.avroData = avroData;
//    }
//
//    @Override
//    public String getExtension() {
//        return EXTENSION;
//    }
//
//    /**
//     * Constructs a RecordWriter that assumes that all {@link SinkRecord} objects provided to it
//     * have the same schema and that the record indeed HAS both a key and value schema.
//     */
//    @Override
//    public RecordWriter getRecordWriter(StorageSinkConnectorConfig conf, String filename) {
//        return new AvroKeyValueWriter(conf, filename);
//    }
//
//    private String getSchemaName(Schema schema) {
//        if (schema != null) {
//            String schemaName = schema.name();
//            if (schemaName != null) {
//                return schemaName;
//            } else {
//                return schema.type().getName();
//            }
//        } else {
//            return null;
//        }
//    }
//
//    private org.apache.avro.Schema getSchema(AvroData avroData, Schema schema) {
//        if (schema == null) {
//            return org.apache.avro.Schema.createUnion(
//                    Arrays.asList(org.apache.avro.Schema.create(NULL), ANYTHING_SCHEMA));
//        } else {
//            return avroData.fromConnectSchema(schema);
//        }
//    }
//
//    private class AvroKeyValueWriter implements RecordWriter {
//        private final DataFileWriter<Object> writer;
//        private final Path path;
//        private final String filename;
//        private final StorageSinkConnectorConfig config;
//        private org.apache.avro.Schema combinedSchema;
//
//        private AvroKeyValueWriter(StorageSinkConnectorConfig config, String filename) {
//            this.filename = filename;
//            this.config = config;
//            writer = new DataFileWriter<>(new GenericDatumWriter<>());
//            path = new Path(filename);
//            combinedSchema = null;
//        }
//
//        @Override
//        public void write(SinkRecord record) {
//            final Schema keySchema = record.keySchema();
//            final Schema valueSchema = record.valueSchema();
//            if (this.combinedSchema == null) {
//                SchemaBuilder.RecordBuilder<org.apache.avro.Schema> builder = SchemaBuilder
//                        .record(getSchemaName(keySchema) + "_" + getSchemaName(valueSchema))
//                        .namespace("org.radarbase.kafka")
//                        .doc("combined key-value record");
//
//                this.combinedSchema = builder.fields()
//                        .name("key").doc("Key of a Kafka SinkRecord")
//                        .type(getSchema(avroData, keySchema)).noDefault()
//                        .name("value").doc("Value of a Kafka SinkRecord")
//                        .type(getSchema(avroData, valueSchema)).noDefault()
//                        .endRecord();
//
//                try {
//                    logger.info("Opening record writer for: {}", filename);
//                    FSDataOutputStream out = path.getFileSystem(config.getHadoopConfiguration())
//                            .create(path);
//                    this.writer.setCodec(CodecFactory.fromString(config.getAvroCodec()));
//                    this.writer.create(combinedSchema, out);
//                } catch (IOException exc) {
//                    throw new ConnectException(exc);
//                }
//            }
//
//            logger.trace("Sink record: {}", record);
//
//            GenericRecord combinedRecord = new GenericData.Record(combinedSchema);
//            write(combinedRecord, 0, keySchema, record.key());
//            write(combinedRecord, 1, valueSchema, record.value());
//            try {
//                this.writer.append(combinedRecord);
//            } catch (IOException exc) {
//                throw new DataException(exc);
//            }
//        }
//
//        private void write(GenericRecord record, int index, Schema schema, Object data) {
//            if (data == null) {
//                return;
//            }
//            Object outputData = avroData.fromConnectData(schema, data);
//            if (outputData instanceof NonRecordContainer) {
//                outputData = ((NonRecordContainer) outputData).getValue();
//            }
//            record.put(index, outputData);
//        }
//
//        @Override
//        public void close() {
//            try {
//                writer.close();
//            } catch (IOException exc) {
//                throw new DataException(exc);
//            }
//
//        }
//
//        @Override
//        public void commit() {
//            // do nothing
//        }
//    }
//}
