package io.contextgraph.query;

import java.time.Duration;
import io.contextgraph.security.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import reactor.core.publisher.*;

public final class LiveBus implements AutoCloseable {
  public record Event(String topic, Map<String,Object> payload) {}
  private final Sinks.Many<Event> events=Sinks.many().multicast().directBestEffort();
  private final KafkaConsumer<String,String> consumer;
  private final Thread thread;
  private final int buffer;
  private volatile boolean running=true, ready=false;
  public LiveBus(String bootstrap,Collection<String> topics,int buffer) {
    this.buffer=buffer;
    if(topics.isEmpty()){consumer=null;thread=null;ready=true;return;}
    Properties p=KafkaSecurity.properties();p.put("bootstrap.servers",bootstrap);p.put("group.id","context-query-"+UUID.randomUUID());p.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");p.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");p.put("auto.offset.reset","latest");p.put("enable.auto.commit","true");p.put("isolation.level","read_committed");p.put("max.poll.records","250");p.put("allow.auto.create.topics","false");
    consumer=new KafkaConsumer<>(p);consumer.subscribe(topics);
    thread=Thread.ofPlatform().name("query-kafka").start(()-> {
      try {while(running) {var records=consumer.poll(Duration.ofMillis(250));ready=consumer.assignment().stream().map(tp->tp.topic()).collect(java.util.stream.Collectors.toSet()).containsAll(topics);for(var r:records) {try {publish(new Event(r.topic(),IcebergQueries.JSON.readValue(r.value(),Map.class)));}catch(Exception e){System.err.println("Invalid live event on "+r.topic()+": "+e.getMessage());}}}}
      catch(WakeupException e) {if(running) throw e;}finally {ready=false;consumer.close();events.tryEmitComplete();}
    });
  }
  public boolean ready(){return ready;}
  void publish(Event event){events.tryEmitNext(event);}
  public static boolean matches(Event event,Collection<String> topics,String field,String entity) {
    if(!topics.contains(event.topic()))return false;
    if(entity==null)return true;
    return entity.equals(event.payload().get(field)) || (entity.equals(event.payload().get("source_id"))||entity.equals(event.payload().get("target_id")));
  }
  public Flux<Map<String,Object>> stream(Collection<String> topics,String field,String entity,boolean wrap,SecurityContext context,PermissionChecks permissions,reactor.core.scheduler.Scheduler scheduler) {
    return events.asFlux().filter(e->matches(e,topics,field,entity)).onBackpressureBuffer(buffer).publishOn(scheduler,1).filter(e->EventAuthorization.visible(e,context,permissions,entity))
      .map(e->{if(!wrap)return e.payload();Map<String,Object> out=new LinkedHashMap<>();out.put("topic",e.topic());out.put("entityId",e.payload().getOrDefault(field,e.payload().get("source_id")));out.put("payload",e.payload());return out;});
  }
  public void close(){running=false;if(consumer==null){ready=false;events.tryEmitComplete();return;}consumer.wakeup();try{thread.join(10000);}catch(InterruptedException e){Thread.currentThread().interrupt();}}
}
