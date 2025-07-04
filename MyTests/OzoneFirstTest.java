package org.example;

import org.apache.commons.lang.builder.StandardToStringStyle;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;


import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class OzoneFirstTest {
    public static void main(String[] args) {

        try {
            OzoneConfiguration conf = new OzoneConfiguration();
            conf.set("ozone.om.address", "localhost");
            conf.set("ozone.security.enabled", "false");
            UserGroupInformation.setConfiguration(conf);
            OzoneClient client = OzoneClientFactory.getRpcClient(conf);

            OzoneClientFactory.getOzoneClient(conf, new Token<>());

                ObjectStore store = client.getObjectStore();
            store.createVolume("vol2");
            OzoneVolume vol = store.getVolume("vol2");
            vol.createBucket("buck1");
            OzoneBucket buck = vol.getBucket("buck1");

            byte[] data = new String("data").getBytes();
            OzoneOutputStream outStream = buck.createKey(new String("key"), data.length);
            outStream.write(data);

            byte[] getted = new byte[data.length];
            OzoneInputStream inStream = buck.readKey("key");
            client.close();


            System.out.println(new String(getted, StandardCharsets.UTF_8));

        } catch (IOException e) {
            System.out.println(e.getMessage() + "\n");
            e.printStackTrace();
        }
    }
}