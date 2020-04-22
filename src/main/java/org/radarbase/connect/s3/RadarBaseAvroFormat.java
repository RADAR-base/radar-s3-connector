/*
 * Copyright 2017 Kings College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.connect.s3;

import static io.confluent.connect.avro.AvroDataConfig.CONNECT_META_DATA_CONFIG;
import static io.confluent.connect.avro.AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG;
import static io.confluent.connect.avro.AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG;

import java.util.Map;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;

/**
 * Extended AvroFormat class to support custom AvroRecordWriter to allow writing key and value to
 * S3.
 */
public class RadarBaseAvroFormat implements Format<S3SinkConnectorConfig, String> {
    private final S3Storage storage;
    private final AvroData avroData;

    public RadarBaseAvroFormat(S3Storage storage) {
        this.storage = storage;
        @SuppressWarnings("unchecked") Map<String, Object> conf =
                (Map<String, Object>) storage.conf().plainValues();

        this.avroData = new AvroData(new AvroDataConfig.Builder()
                .with(CONNECT_META_DATA_CONFIG, conf.getOrDefault(CONNECT_META_DATA_CONFIG, false))
                .with(SCHEMAS_CACHE_SIZE_CONFIG, conf.getOrDefault(SCHEMA_CACHE_SIZE_CONFIG, 1000))
                .with(ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG,
                        conf.getOrDefault(ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)).build());
    }

    public RecordWriterProvider<S3SinkConnectorConfig> getRecordWriterProvider() {
        return new AvroKeyValueWriterProvider(this.storage, this.avroData);
    }

    public SchemaFileReader<S3SinkConnectorConfig, String> getSchemaFileReader() {
        throw new UnsupportedOperationException(
                "Reading schemas from S3 is not currently supported");
    }

    /**
     * @deprecated
     */
    @Deprecated
    public Object getHiveFactory() {
        throw new UnsupportedOperationException(
                "Hive integration is not currently supported in S3 Connector");
    }

    public AvroData getAvroData() {
        return this.avroData;
    }
}
