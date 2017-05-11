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
package zipkin.autoconfigure.storage.cassandra3.brave;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.SamplingFlags;
import brave.propagation.TraceContext;
import com.datastax.driver.core.AbstractSession;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.base.CaseFormat;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import zipkin.Endpoint;

import static zipkin.Constants.ERROR;
import static zipkin.internal.Util.checkNotNull;

final class TracingSession extends AbstractSession {
  static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
  static final String CASSANDRA_QUERY = "cassandra.query";

  final Tracer tracer;
  final String remoteServiceName;
  final TraceContext.Injector<Map<String, ByteBuffer>> injector;
  final ProtocolVersion version;
  final Session delegate;

  TracingSession(Tracing tracing, Session target) {
    tracer = tracing.tracer();
    delegate = checkNotNull(target, "delegate");
    remoteServiceName = "cassandra";
    injector = tracing.propagation().injector((carrier, key, v) -> {
      int length = v.length(); // all values are ascii
      byte[] buf = new byte[length];
      for (int i = 0; i < length; i++) {
        buf[i] = (byte) v.charAt(i);
      }
      carrier.put(key, ByteBuffer.wrap(buf));
    });
    version = delegate.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
  }

  @Override public ResultSetFuture executeAsync(Statement statement) {
    Span span = tracer.currentSpan() != null
        ? tracer.nextSpan()
        : tracer.newTrace(SamplingFlags.NOT_SAMPLED); // don't start new traces for client requests

    if (!span.isNoop()) {
      span.name(spanName(statement));
      String keyspace = statement.getKeyspace();
      if (keyspace != null) {
        span.tag(CASSANDRA_KEYSPACE, statement.getKeyspace());
      }
      if (statement instanceof BoundStatement) {
        span.tag(CASSANDRA_QUERY,
            ((BoundStatement) statement).preparedStatement().getQueryString());
      }
    }

    // o.a.c.tracing.Tracing.newSession must use the same propagation format
    if (version.compareTo(ProtocolVersion.V4) >= 0) {
      statement.enableTracing();
      Map<String, ByteBuffer> payload = new LinkedHashMap<>();
      if (statement.getOutgoingPayload() != null) {
        payload.putAll(statement.getOutgoingPayload());
      }
      injector.inject(span.context(), payload);
      statement.setOutgoingPayload(payload);
    }

    span.start();
    ResultSetFuture result;
    try {
      result = delegate.executeAsync(statement);
    } catch (RuntimeException | Error e) {
      if (span.isNoop()) throw e;
      error(e, span);
      span.finish();
      throw e;
    }
    if (span.isNoop()) return result; // don't add callback on noop
    Futures.addCallback(result, new FutureCallback<ResultSet>() {
      @Override public void onSuccess(ResultSet result) {
        InetSocketAddress host = result.getExecutionInfo().getQueriedHost().getSocketAddress();
        Endpoint.Builder remoteEndpoint = Endpoint.builder().serviceName(remoteServiceName);
        remoteEndpoint.parseIp(host.getAddress());
        remoteEndpoint.port(host.getPort());
        span.remoteEndpoint(remoteEndpoint.build());
        span.finish();
      }

      @Override public void onFailure(Throwable e) {
        error(e, span);
        span.finish();
      }
    });
    return result;
  }

  @Override protected ListenableFuture<PreparedStatement> prepareAsync(String query,
      Map<String, ByteBuffer> customPayload) {
    SimpleStatement statement = new SimpleStatement(query);
    statement.setOutgoingPayload(customPayload);
    return prepareAsync(statement);
  }

  @Override public ListenableFuture<PreparedStatement> prepareAsync(String query) {
    return delegate.prepareAsync(query);
  }

  @Override public String getLoggedKeyspace() {
    return delegate.getLoggedKeyspace();
  }

  @Override public Session init() {
    return delegate.init();
  }

  @Override public ListenableFuture<Session> initAsync() {
    return delegate.initAsync();
  }

  @Override public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
    return delegate.prepareAsync(statement);
  }

  @Override public CloseFuture closeAsync() {
    return delegate.closeAsync();
  }

  @Override public boolean isClosed() {
    return delegate.isClosed();
  }

  @Override public Cluster getCluster() {
    return delegate.getCluster();
  }

  @Override public State getState() {
    return delegate.getState();
  }

  static void error(Throwable throwable, Span span) {
    String message = throwable.getMessage();
    span.tag(ERROR, message != null ? message : throwable.getClass().getSimpleName());
  }

  /** Returns the span name of the statement. Defaults to the lower-camel case type name. */
  static String spanName(Statement statement) {
    return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, statement.getClass().getSimpleName());
  }
}
