/*
 * Copyright 2021 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.eventstreams.connect.cossink.partitionwriter;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cos.Bucket;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class COSOutputStreamTest {

    @Test
    public void testClose() throws IOException {
        Bucket mockBucket = mock(Bucket.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ByteArrayInputStream> streamCaptor = ArgumentCaptor.forClass(ByteArrayInputStream.class);
        ArgumentCaptor<ObjectMetadata> metadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        String key = "some-name";
        String data = "something";

        COSOutputStream stream = new COSOutputStream(
          mockBucket, key, 100
        );
        

        for (byte b: data.getBytes()) {
          stream.write((int)b);
        }
        assertEquals(data.length(), stream.getPos());
        stream.close();
        verify(mockBucket).putObject(keyCaptor.capture(), streamCaptor.capture(), metadataCaptor.capture());
        assertEquals(key, keyCaptor.getValue());
        assertEquals(data.length(), metadataCaptor.getValue().getContentLength());
        assertEquals(data.length(), streamCaptor.getValue().available());
    }

}
