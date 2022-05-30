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

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class DependenciesSparkJob {
    private static ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);

    public static void main(String[] args) throws UnsupportedEncodingException {
        String storage = System.getenv("STORAGE");
        if (storage == null) {
            throw new IllegalArgumentException("Missing environmental variable STORAGE");
        }
        SparkTask sparkTask = new SparkTask();
        sparkTask.setStorage(storage);
        scheduledExecutorService.scheduleWithFixedDelay(sparkTask, 10, 10, TimeUnit.SECONDS);
    }


}
