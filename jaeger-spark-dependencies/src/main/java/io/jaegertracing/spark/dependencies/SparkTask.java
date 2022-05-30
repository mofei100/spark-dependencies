/**
 * Copyright 2017 The Jaeger Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.jaegertracing.spark.dependencies;


import io.jaegertracing.spark.dependencies.cassandra.CassandraDependenciesJob;
import io.jaegertracing.spark.dependencies.elastic.ElasticsearchDependenciesJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.time.LocalDate;

public class SparkTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SparkTask.class);

    private String storage;

    public String getStorage() {
        return storage;
    }

    public void setStorage(String storage) {
        this.storage = storage;
    }


    @Override
    public void run() {
        log.info("task run start");
        try {
            LocalDate date = LocalDate.now();
            task(storage, date);
        } catch (UnsupportedEncodingException e) {
            log.error("task running error,msg:{},cause:{}", e.getMessage(), e.getCause());
        }
        log.info("task run end");
    }

    private void task(String storage, LocalDate localDate) throws UnsupportedEncodingException {
        String peerServiceTag = System.getenv("PEER_SERVICE_TAG");
        if (peerServiceTag == null) {
            peerServiceTag = "peer.service";
        }
        String jarPath = pathToUberJar();
        if ("elasticsearch".equalsIgnoreCase(storage)) {
            ElasticsearchDependenciesJob.builder()
                    .jars(jarPath)
                    .day(localDate)
                    .build()
                    .run(peerServiceTag);
        } else if ("cassandra".equalsIgnoreCase(storage)) {
            CassandraDependenciesJob.builder()
                    .jars(jarPath)
                    .day(localDate)
                    .build()
                    .run(peerServiceTag);
        } else {
            throw new IllegalArgumentException("Unsupported storage: " + storage);
        }
    }

    String pathToUberJar() throws UnsupportedEncodingException {
        URL jarFile = DependenciesSparkJob.class.getProtectionDomain().getCodeSource().getLocation();
        return URLDecoder.decode(jarFile.getPath(), "UTF-8");
    }

    LocalDate parseZonedDateTime(String date) {
        return LocalDate.parse(date);
    }
}
