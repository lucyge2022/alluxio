/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.metrics.sink;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A sink that exposes metrics data in JSON format by HTTP.
 */
@NotThreadSafe
public class MetricsServlet implements Sink {
  public static final String SERVLET_PATH = "/metrics/json";

  private MetricRegistry mMetricsRegistry;
  private ObjectMapper mObjectMapper;

  /**
   * Creates a new {@link MetricsServlet} with a {@link Properties} and {@link MetricRegistry}.
   *
   * @param registry the metric registry to register
   */
  public MetricsServlet(MetricRegistry registry) {
    mMetricsRegistry = registry;
    mObjectMapper =
        new ObjectMapper().registerModule(new MetricsModule(TimeUnit.SECONDS,
            TimeUnit.MILLISECONDS, false));
  }

  private HttpServlet createServlet() {
    return new HttpServlet() {
      private static final long serialVersionUID = -2761243531478788172L;

      @Override
      protected void doGet(HttpServletRequest request, HttpServletResponse response)
          throws ServletException, IOException {
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
        String result = mObjectMapper.writerWithDefaultPrettyPrinter()
            .writeValueAsString(mMetricsRegistry);
        response.getWriter().println(result);
      }
    };
  }

  /**
   * Gets the {@link ServletContextHandler} of the metrics servlet.
   *
   * @return the {@link ServletContextHandler} object
   */
  public ServletContextHandler getHandler() {
    ServletContextHandler contextHandler = new ServletContextHandler();
    contextHandler.setContextPath(SERVLET_PATH);
    contextHandler.addServlet(new ServletHolder(createServlet()), "/");
    return contextHandler;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void report() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Timer> entry : mMetricsRegistry.getTimers().entrySet()) {
      Snapshot snapshot = entry.getValue().getSnapshot();
      long[] values = snapshot.getValues();
      long[] buckets = new long[15];
      for (long value : values) {
        long valueInMillis = TimeUnit.NANOSECONDS.toMillis(value);
        int idx = valueInMillis == 0L ? 0 : (Long.numberOfTrailingZeros(valueInMillis) + 1);
        ++buckets[idx];
      }
      sb.append("\ncount" + ":" + entry.getValue().getCount());
      double durationFactor = 1.0 / TimeUnit.MILLISECONDS.toNanos(1);
      sb.append("\nmax" + ":" + snapshot.getMax() * durationFactor);
      sb.append("\nmean" + ":" + snapshot.getMean() * durationFactor);
      sb.append("\nmin" + ":" + snapshot.getMin() * durationFactor);

      sb.append("\np50" + ":" + snapshot.getMedian() * durationFactor);
      sb.append("\np95" + ":" + snapshot.get95thPercentile() * durationFactor);
      sb.append("\nduration_units" + ":" + TimeUnit.MILLISECONDS.toString().toLowerCase(Locale.US));
      int st = 0;
      int ed = 0;
      for (int i=0; i<buckets.length; ++i) {
        ed = 1 << i;
        sb.append("\n[" + st + "-" + ed + "]:" + buckets[i]);
        st = ed;
      }
    }
    String output = sb.toString();
  }
}
