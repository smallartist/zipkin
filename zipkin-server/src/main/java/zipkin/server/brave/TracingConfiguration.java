/**
 * Copyright 2015-2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.server.brave;

import brave.Tracing;
import brave.context.slf4j.MDCCurrentTraceContext;
import brave.http.HttpAdapter;
import brave.http.HttpSampler;
import brave.http.HttpTracing;
import brave.propagation.CurrentTraceContext;
import brave.sampler.BoundarySampler;
import brave.sampler.Sampler;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import zipkin.Span;
import zipkin.collector.CollectorMetrics;
import zipkin.reporter.AsyncReporter;
import zipkin.reporter.Reporter;
import zipkin.reporter.ReporterMetrics;
import zipkin.reporter.Sender;
import zipkin.reporter.local.LocalSender;
import zipkin.server.ConditionalOnSelfTracing;
import zipkin.storage.StorageComponent;

@Configuration
@ConditionalOnSelfTracing
@Import(TracingWebMvcConfigurerAdapter.class)
public class TracingConfiguration {

  // Note: there's a chicken or egg problem here. TracingStorageComponent wraps StorageComponent with
  // Brave. During initialization, if we eagerly reference StorageComponent from within Brave,
  // BraveTracedStorageComponentEnhancer won't be able to process it. TL;DR; if you take out Lazy
  // here, self-tracing will not affect the storage component, which reduces its effectiveness.
  @Bean Sender sender(@Lazy StorageComponent storage) {
    return LocalSender.create(storage);
  }

  /** Configuration for how to buffer spans into messages for Zipkin */
  @Bean Reporter<Span> reporter(Sender sender,
      @Value("${zipkin.self-tracing.message-timeout:1}") int messageTimeout,
      CollectorMetrics metrics) {
    return AsyncReporter.builder(sender)
        .messageTimeout(messageTimeout, TimeUnit.SECONDS)
        .metrics(new ReporterMetricsAdapter(metrics.forTransport("local"))).build();
  }

  @Bean CurrentTraceContext currentTraceContext() {
    return MDCCurrentTraceContext.create(); // puts trace IDs into logs
  }

  /** Controls aspects of tracing such as the name that shows up in the UI */
  @Bean Tracing tracing(Reporter<Span> reporter,
      @Value("${zipkin.self-tracing.sample-rate:1.0}") float rate) {
    return Tracing.newBuilder()
        .localServiceName("zipkin-server")
        .sampler(rate < 0.01 ? BoundarySampler.create(rate) : Sampler.create(rate))
        .currentTraceContext(currentTraceContext())
        .reporter(reporter).build();
  }

  @Bean HttpTracing httpTracing(Tracing tracing) {
    return HttpTracing.newBuilder(tracing)
        // server starts traces for read requests under the path /api
        .serverSampler(new HttpSampler() {
          @Override public <Req> Boolean trySample(HttpAdapter<Req, ?> adapter, Req request) {
            return "GET".equals(adapter.method(request))
                && adapter.path(request).startsWith("/api");
          }
        })
        // client doesn't start new traces
        .clientSampler(HttpSampler.NEVER_SAMPLE).build();
  }

  static final class ReporterMetricsAdapter implements ReporterMetrics {
    final CollectorMetrics delegate;

    ReporterMetricsAdapter(CollectorMetrics delegate) {
      this.delegate = delegate;
    }

    @Override public void incrementMessages() {
      delegate.incrementMessages();
    }

    @Override public void incrementMessagesDropped(Throwable throwable) {
      delegate.incrementMessagesDropped();
    }

    @Override public void incrementSpans(int i) {
      delegate.incrementSpans(i);
    }

    @Override public void incrementSpanBytes(int i) {
      delegate.incrementBytes(i);
    }

    @Override public void incrementMessageBytes(int i) {
    }

    @Override public void incrementSpansDropped(int i) {
      delegate.incrementMessagesDropped();
    }

    @Override public void updateQueuedSpans(int i) {
    }

    @Override public void updateQueuedBytes(int i) {
    }
  }
}
