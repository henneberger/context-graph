package io.contextgraph.security;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Bounded, payload-free Prometheus metrics on a dedicated internal port. */
public final class HttpMetrics {
  private static final ConcurrentHashMap<String, Stats> METRICS=new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, LongAdder> EVENTS=new ConcurrentHashMap<>();
  private static final double[] BOUNDS={.005,.01,.025,.05,.1,.25,.5,1,2.5,5,10,30,120,3600};
  private static final AtomicBoolean STARTED=new AtomicBoolean();
  private static class Stats {final LongAdder count=new LongAdder();final DoubleAdder sum=new DoubleAdder();final LongAdder[] buckets=java.util.stream.IntStream.range(0,BOUNDS.length).mapToObj(i->new LongAdder()).toArray(LongAdder[]::new);}
  public static void start() {
    if(!STARTED.compareAndSet(false,true))return;
    try {
      var server=HttpServer.create(new InetSocketAddress(Integer.parseInt(System.getenv().getOrDefault("METRICS_PORT","9404"))),16);
      server.createContext("/metrics",exchange->{if(!exchange.getRequestMethod().equals("GET")||!exchange.getRequestURI().getPath().equals("/metrics")){exchange.sendResponseHeaders(404,-1);exchange.close();return;}byte[] bytes=render().getBytes(StandardCharsets.UTF_8);exchange.getResponseHeaders().set("Content-Type","text/plain; version=0.0.4; charset=utf-8");exchange.sendResponseHeaders(200,bytes.length);try(var out=exchange.getResponseBody()){out.write(bytes);}});
      server.setExecutor(Executors.newFixedThreadPool(2,r->{Thread t=new Thread(r,"metrics");t.setDaemon(true);return t;}));server.start();
      Runtime.getRuntime().addShutdownHook(new Thread(()->server.stop(1)));
    }catch(Exception e){throw new IllegalStateException("Metrics listener failed",e);}
  }
  public static String route(String path){if(path.startsWith("/media/"))return "media";if(path.startsWith("/health/"))return "health";if(path.equals("/graphql"))return "graphql";if(path.startsWith("/auth/"))return "auth";if(path.startsWith("/ingest/"))return "ingest";return "other";}
  public static void observe(String route,int status,long started){String key="route=\""+route+"\",status=\""+Math.max(100,Math.min(599,status))+"\"";Stats s=METRICS.computeIfAbsent(key,k->new Stats());double seconds=(System.nanoTime()-started)/1e9;s.count.increment();s.sum.add(seconds);for(int i=0;i<BOUNDS.length;i++)if(seconds<=BOUNDS[i])s.buckets[i].increment();}
  public static void event(String event){if(!java.util.Set.of("graphql_error","kafka_publish_success","kafka_publish_failure").contains(event))throw new IllegalArgumentException();EVENTS.computeIfAbsent(event,k->new LongAdder()).increment();}
  static String render(){StringBuilder out=new StringBuilder("# TYPE context_http_requests_total counter\n# TYPE context_http_request_duration_seconds histogram\n");METRICS.forEach((k,s)->{out.append("context_http_requests_total{").append(k).append("} ").append(s.count.sum()).append('\n');for(int i=0;i<BOUNDS.length;i++)out.append("context_http_request_duration_seconds_bucket{").append(k).append(",le=\"").append(BOUNDS[i]).append("\"} ").append(s.buckets[i].sum()).append('\n');out.append("context_http_request_duration_seconds_bucket{").append(k).append(",le=\"+Inf\"} ").append(s.count.sum()).append('\n');out.append("context_http_request_duration_seconds_sum{").append(k).append("} ").append(s.sum.sum()).append('\n');out.append("context_http_request_duration_seconds_count{").append(k).append("} ").append(s.count.sum()).append('\n');});out.append("# TYPE context_service_events_total counter\n");EVENTS.forEach((k,v)->out.append("context_service_events_total{event=\"").append(k).append("\"} ").append(v.sum()).append('\n'));out.append("# TYPE context_jvm_heap_used_bytes gauge\ncontext_jvm_heap_used_bytes ").append(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed()).append('\n');out.append("# TYPE context_jvm_threads gauge\ncontext_jvm_threads ").append(ManagementFactory.getThreadMXBean().getThreadCount()).append('\n');out.append("# TYPE context_process_uptime_seconds gauge\ncontext_process_uptime_seconds ").append(ManagementFactory.getRuntimeMXBean().getUptime()/1000d).append('\n');return out.toString();}
}
