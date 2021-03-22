package com.datorama.oss.timbermill.common.redis;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BulkRequestSerializer extends Serializer<BulkRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(BulkRequestSerializer.class);

    @Override
    public void write(Kryo kryo, Output output, BulkRequest request) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            kryo.writeClassAndObject(output, out.bytes().toBytesRef().bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public BulkRequest read(Kryo kryo, Input input, Class<? extends BulkRequest> aClass) {
        try (StreamInput stream = StreamInput.wrap((byte[]) kryo.readClassAndObject(input))) {
            return new BulkRequest(stream);
        } catch (IOException e) {
            LOG.error("Kryo has failed to deserialize a bulk request");
            return null;
        }
    }
}
