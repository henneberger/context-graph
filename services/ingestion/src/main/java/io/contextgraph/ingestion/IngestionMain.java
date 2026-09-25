package io.contextgraph.ingestion;

import io.vertx.core.*;
import io.contextgraph.security.*;
import io.vertx.core.http.*;
import io.vertx.core.json.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import javax.imageio.ImageIO;
import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class IngestionMain {
    private final AccessControl access = AccessControl.fromEnvironment();
    private final Vertx vertx = Vertx.vertx();
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final JsonObject config;
    private final Path configDir, media;
    private final ObjectMedia objects;
    private final SchemaCheck envelope, error;
    private final Map<String, Endpoint> endpoints = new HashMap<>();
    private final KafkaProducer<String,String> producer;
    private final Semaphore permits;
    private final AtomicBoolean draining = new AtomicBoolean();
    private final AtomicBoolean kafkaReady = new AtomicBoolean();
    private final AtomicBoolean checkingKafka = new AtomicBoolean();
    private HttpServer server;
    record Endpoint(String name, String kind, String topic, SchemaCheck schema, String resourcePointer, String eventTimePointer) {}
    record Authorized(SecurityContext context,String entityId,String resourceId) {}
    private Authorized authorize(SecurityContext context,String entity) {
        String resource=AccessControl.resourceId(context.workspaceId(),entity); access.require(context,resource,"ingest");
        return new Authorized(context,entity,resource);
    }

    IngestionMain(Path configPath) throws Exception {
        configDir = configPath.toAbsolutePath().getParent();
        config = new JsonObject(Files.readString(configPath));
        media = Path.of(env("MEDIA_ROOT", config.getString("mediaRoot", "/data/media"))).toAbsolutePath();
        Files.createDirectories(media);
        objects=System.getenv("MEDIA_S3_ENDPOINT")==null?null:new ObjectMedia(media);
        envelope = new SchemaCheck(configDir.resolve(config.getString("envelopeSchema")));
        error = new SchemaCheck(configDir.resolve(config.getString("errorSchema")));
        permits = new Semaphore(config.getInteger("maxConcurrentUploads", 8));
        Set<String> topics = new HashSet<>();
        for (Object item : config.getJsonArray("endpoints")) {
            JsonObject e = (JsonObject)item;
            if (!Set.of("json", "image", "video").contains(e.getString("kind"))) throw new IllegalArgumentException("Unsupported endpoint kind");
            Endpoint endpoint = new Endpoint(e.getString("name"), e.getString("kind"), e.getString("topic"), e.containsKey("schema") ? new SchemaCheck(configDir.resolve(e.getString("schema"))) : null,e.getString("resourcePointer",""),e.getString("eventTimePointer",""));
            if (endpoint.kind.equals("json") && endpoint.schema == null) throw new IllegalArgumentException("JSON endpoints require a schema");
            if (!topics.add(endpoint.topic)) throw new IllegalArgumentException("Each endpoint requires a distinct Kafka topic");
            if (endpoints.put(e.getString("path"), endpoint) != null) throw new IllegalArgumentException("Duplicate endpoint path");
        }
        Properties props = KafkaSecurity.properties();
        props.put("bootstrap.servers", env("KAFKA_BOOTSTRAP_SERVERS", config.getString("kafkaBootstrapServers")));
        props.put("key.serializer", StringSerializer.class.getName()); props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all"); props.put("enable.idempotence", "true"); props.put("max.block.ms", "15000"); props.put("delivery.timeout.ms", "30000"); props.put("request.timeout.ms", "15000");
        producer = new KafkaProducer<>(props);
    }
    private static String env(String name, String fallback) { return System.getenv().getOrDefault(name, fallback); }
    public static void main(String[] args) throws Exception {
        IngestionMain app = new IngestionMain(Path.of(env("CONFIG_PATH", "config/ingestion.json")));
        app.start(); Runtime.getRuntime().addShutdownHook(new Thread(app::stop));
    }
    private void start() throws Exception {
        io.contextgraph.security.HttpMetrics.start();
        vertx.setPeriodic(1000, timer -> {
            if (!checkingKafka.compareAndSet(false,true) || draining.get()) return;
            workers.submit(() -> {
                try { for (Endpoint e : endpoints.values()) producer.partitionsFor(e.topic); producer.partitionsFor(config.getString("errorTopic")); kafkaReady.set(true); }
                catch(Exception e) { kafkaReady.set(false); }
                finally { checkingKafka.set(false); }
            });
        });
        HttpServerOptions options=new HttpServerOptions().setIdleTimeout(75).setCompressionSupported(true);
        String keystore=System.getenv("TLS_KEYSTORE_PATH");
        if(keystore!=null && !keystore.isBlank()) {
            String password=System.getenv("TLS_KEYSTORE_PASSWORD");
            if(password==null || password.isBlank()) throw new IllegalStateException("TLS_KEYSTORE_PASSWORD required");
            options.setSsl(true).setKeyCertOptions(new io.vertx.core.net.PfxOptions().setPath(keystore).setPassword(password));
        } else if(!"true".equals(System.getenv("SECURITY_ALLOW_HTTP"))) throw new IllegalStateException("Server TLS required; HTTP requires explicit development setting");
        server = vertx.createHttpServer(options);
        server.requestHandler(this::handle).listen(Integer.parseInt(env("PORT", config.getInteger("port",8080).toString()))).toCompletionStage().toCompletableFuture().get();
    }
    private void handle(HttpServerRequest request) {
        String path = request.path();
        long metricStart=System.nanoTime();
        request.response().bodyEndHandler(v->io.contextgraph.security.HttpMetrics.observe(io.contextgraph.security.HttpMetrics.route(path),request.response().getStatusCode(),metricStart));
        if (path.equals("/health/live") || path.equals("/health/ready")) { request.response().setStatusCode(draining.get() || (path.equals("/health/ready") && !kafkaReady.get()) ? 503 : 200).end("ok"); return; }
        if ((request.method() == HttpMethod.GET || request.method() == HttpMethod.HEAD) && path.startsWith("/media/")) {
            workers.submit(() -> { try { serveMedia(request); } catch(Exception e) { reject(request,e); } }); return;
        }
        if (request.method() == HttpMethod.POST && path.equals("/auth/logout")) {
            String cookie="cg_media_session=; Path=/media/; Max-Age=0; HttpOnly; SameSite=Strict"+("true".equals(System.getenv("SECURITY_ALLOW_HTTP"))?"":"; Secure");
            request.response().putHeader("Set-Cookie",cookie).putHeader("Cache-Control","private, no-store").setStatusCode(204).end(); return;
        }
        if (request.method() == HttpMethod.POST && path.equals("/auth/session")) {
            request.pause(); workers.submit(() -> { try { session(request); } catch(Exception e) { reject(request,e); } }); return;
        }
        Endpoint endpoint = endpoints.get(path);
        if (endpoint == null || request.method() != HttpMethod.POST) { request.response().setStatusCode(404).end(); return; }
        if (draining.get() || !permits.tryAcquire()) { request.response().setStatusCode(503).putHeader("Retry-After","2").end(); return; }
        request.pause();
        workers.submit(() -> {
            AtomicReference<Authorized> authorized=new AtomicReference<>();
            try {
                SecurityContext context=access.authenticate(request.getHeader("Authorization"),request.getHeader("X-Workspace-Id"));
                if (endpoint.kind.equals("json")) json(request, endpoint,context,authorized);
                else {
                    Authorized auth=authorize(context,request.getHeader("X-Entity-Id")); authorized.set(auth);
                    if (endpoint.kind.equals("image")) image(request,endpoint,auth); else video(request,endpoint,auth);
                }
            } catch (Exception e) { failure(request, endpoint, e,authorized.get()); }
            finally { permits.release(); }
        });
    }
    private void receive(HttpServerRequest request, OutputStream out, long limit,SecurityContext context,Authorized auth) throws Exception {
        CompletableFuture<Void> done = new CompletableFuture<>(); AtomicLong size = new AtomicLong();
        AtomicBoolean checking=new AtomicBoolean();
        long timer=vertx.setPeriodic(1000, tick -> {
            if(!checking.compareAndSet(false,true)) return;
            workers.submit(() -> { try { if(auth==null) access.requireWorkspace(context,"access"); else access.require(context,auth.resourceId,"ingest"); }
                catch(Exception e) { done.completeExceptionally(e); } finally { checking.set(false); } });
        });
        vertx.runOnContext(ignored -> {
            request.exceptionHandler(done::completeExceptionally);
            request.handler(buffer -> {
                request.pause();
                workers.submit(() -> {
                    try {
                        if(done.isDone()) return;
                        if(context.expiresAtEpochSecond()<=Instant.now().getEpochSecond()) throw new AuthException(401,"Credentials expired");
                        if (size.addAndGet(buffer.length()) > limit) throw new IllegalArgumentException("Upload exceeds byte limit");
                        out.write(buffer.getBytes());
                        vertx.runOnContext(v -> request.resume());
                    } catch (Exception e) { done.completeExceptionally(e); }
                });
            });
            request.endHandler(v -> done.complete(null)); request.resume();
        });
        try { done.get(config.getInteger("maxVideoSeconds",3600), TimeUnit.SECONDS); out.flush(); }
        finally { vertx.cancelTimer(timer); }
    }
    private static String pointer(Object payload,String pointer,String fallback) throws Exception {
        if(!pointer.startsWith("/"))throw new IllegalArgumentException("JSON Pointer must begin with /");
        var value=new com.fasterxml.jackson.databind.ObjectMapper().readTree(io.vertx.core.json.Json.encode(payload)).at(pointer);
        if(value.isMissingNode())return fallback;
        if(!value.isTextual())throw new IllegalArgumentException("Bound resource/time field must be a string");
        return value.textValue();
    }
    private JsonObject event(Endpoint endpoint, Authorized auth, Object payload) throws Exception {
        String now = Instant.now().toString(); String eventTime = endpoint.eventTimePointer.isBlank()?now:pointer(payload,endpoint.eventTimePointer,now);
        Instant.parse(eventTime);
        return new JsonObject().put("eventId", UUID.randomUUID().toString()).put("schemaVersion",2).put("endpoint",endpoint.name).put("kind",endpoint.kind)
                .put("entityId",auth.entityId).put("eventTime",eventTime).put("ingestedAt",now).put("payload",payload).put("security",MediaProvenance.labels(auth.context,auth.entityId));
    }
    private void publish(Endpoint endpoint, JsonObject event,Authorized auth) throws Exception {
        envelope.validate(event); access.require(auth.context,auth.resourceId,"ingest");
        producer.send(new ProducerRecord<>(endpoint.topic,auth.context.workspaceId()+"\u0000"+auth.resourceId,event.encode())).get(35,TimeUnit.SECONDS);
    }
    static Object parsePayload(String json) throws Exception {
        var mapper=new com.fasterxml.jackson.databind.ObjectMapper().enable(com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        return mapper.readValue(json,Object.class);
    }
    private void json(HttpServerRequest request, Endpoint endpoint,SecurityContext context,AtomicReference<Authorized> authorized) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(); receive(request,out,config.getLong("maxJsonBytes",900000L),context,null);
        Object payload = parsePayload(out.toString(java.nio.charset.StandardCharsets.UTF_8));
        Authorized auth=authorize(context,endpoint.resourcePointer.isBlank()?request.getHeader("X-Resource-Key"):pointer(payload,endpoint.resourcePointer,null)); authorized.set(auth); endpoint.schema.validate(payload);
        JsonObject event = event(endpoint,auth,payload);
        publish(endpoint,event,auth); respond(request,202,new JsonObject().put("eventId",event.getString("eventId")));
    }
    private void image(HttpServerRequest request, Endpoint endpoint,Authorized auth) throws Exception {
        String id = UUID.randomUUID().toString(); Path pending = media.resolve(id + ".tmp");
        try {
            try (OutputStream out = Files.newOutputStream(pending)) { receive(request,out,config.getLong("maxImageBytes",20971520L),auth.context,auth); }
            ImageMetadata.requireSupportedSignature(pending);
            int width, height; String format;
            try (var input = ImageIO.createImageInputStream(pending.toFile())) {
                var readers = ImageIO.getImageReaders(input);
                if (!readers.hasNext()) throw new IllegalArgumentException("Unsupported image");
                var reader = readers.next();
                try { reader.setInput(input); width = reader.getWidth(0); height = reader.getHeight(0); format = reader.getFormatName().toLowerCase(Locale.ROOT); }
                finally { reader.dispose(); }
            }
            if (!Set.of("png","jpeg","jpg").contains(format)) throw new IllegalArgumentException("Only PNG and JPEG supported");
            Path target = media.resolve(id + (format.equals("png") ? ".png" : ".jpg"));
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(pending)) { byte[] b = new byte[65536]; int n; while ((n = input.read(b)) != -1) digest.update(b,0,n); }
            forceFile(pending);
            Files.move(pending,target,StandardCopyOption.ATOMIC_MOVE);
            forceDirectory(media);
            JsonObject payload = new JsonObject().put("uri","/media/"+target.getFileName()).put("width",width).put("height",height).put("contentType",format.equals("png")?"image/png":"image/jpeg")
                    .put("bytes",Files.size(target)).put("sha256",HexFormat.of().formatHex(digest.digest()));
            writeSidecar(MediaProvenance.sidecar(media,target),auth);
            if(objects!=null)objects.put(target);
            JsonObject event = event(endpoint,auth,payload); publish(endpoint,event,auth); respond(request,202,event);
            if(objects!=null){Files.deleteIfExists(target);Files.deleteIfExists(MediaProvenance.sidecar(media,target));}
        } finally { Files.deleteIfExists(pending); }
    }
    private static void forceFile(Path file) throws IOException {
        try (var channel=java.nio.channels.FileChannel.open(file,StandardOpenOption.WRITE)) { channel.force(true); }
    }
    private static void forceDirectory(Path directory) throws IOException {
        try (var channel=java.nio.channels.FileChannel.open(directory,StandardOpenOption.READ)) { channel.force(true); }
    }
    private void video(HttpServerRequest request, Endpoint endpoint,Authorized auth) throws Exception {
        String id=UUID.randomUUID().toString(); Path folder=media.resolve(id); Files.createDirectory(folder); forceDirectory(media);
        writeSidecar(folder.resolve(".security.json"),auth);
        String playlist="/media/"+id+"/index.m3u8";
        Process process = new ProcessBuilder(VideoSegments.command(folder,config.getInteger("segmentSeconds",2))).redirectError(folder.resolve("ffmpeg.log").toFile()).start();
        AtomicBoolean finished = new AtomicBoolean(); AtomicInteger emitted = new AtomicInteger();
        CompletableFuture<Void> monitor = CompletableFuture.runAsync(() -> {
            try {
                do {
                    boolean finalScan = finished.get();
                    for (var segment : VideoSegments.read(folder.resolve("index.m3u8"))) {
                        if (segment.sequence() < emitted.get()) continue;
                        forceFile(folder.resolve(segment.filename()));
                        forceDirectory(folder);
                        JsonObject payload=new JsonObject().put("streamId",id).put("sequence",segment.sequence()).put("uri","/media/"+id+"/"+segment.filename())
                                .put("playlistUri",playlist).put("durationSeconds",segment.duration()).put("keyframeAligned",true);
                        if(objects!=null){objects.put(folder.resolve(segment.filename()));publishPlaylist(folder,segment.sequence()+1,false);}
                        publish(endpoint,event(endpoint,auth,payload),auth); emitted.incrementAndGet();
                        if(objects!=null)Files.deleteIfExists(folder.resolve(segment.filename()));
                    }
                    if (finalScan) {if(objects!=null)publishPlaylist(folder,emitted.get(),true);break;}
                    Thread.sleep(200);
                } while (true);
            } catch (Exception e) { process.destroyForcibly(); throw new CompletionException(e); }
        },workers);
        vertx.runOnContext(v -> request.response().setChunked(true).putHeader("Content-Type","application/x-ndjson")
                .putHeader("X-Stream-Id",id).putHeader("X-Playlist-Uri",playlist).write(new JsonObject().put("status","streaming").put("streamId",id).put("playlistUri",playlist).encode()+"\n"));
        try {
            try (OutputStream stdin=process.getOutputStream()) { receive(request,stdin,config.getLong("maxVideoBytes",10737418240L),auth.context,auth); }
            if (!process.waitFor(30,TimeUnit.SECONDS)) throw new IOException("FFmpeg finalization timed out");
            if (process.exitValue()!=0) throw new IllegalArgumentException("Video decoding failed; inspect stream log " + id);
            finished.set(true); monitor.get(40,TimeUnit.SECONDS);
            if (emitted.get()==0) throw new IllegalArgumentException("Video contains no frames");
            respond(request,200,new JsonObject().put("status","complete").put("streamId",id).put("segments",emitted.get()).put("playlistUri",playlist));
        } finally { process.destroyForcibly(); finished.set(true); if(objects!=null){try{monitor.get(10,TimeUnit.SECONDS);}catch(Exception ignored){}try(var paths=Files.walk(folder)){for(Path path:paths.sorted(Comparator.reverseOrder()).toList())Files.deleteIfExists(path);}} }
    }
    private void publishPlaylist(Path folder,int count,boolean ended)throws Exception {
        var segments=VideoSegments.read(folder.resolve("index.m3u8")).stream().filter(segment->segment.sequence()<count).toList();
        int duration=segments.stream().mapToInt(segment->(int)Math.ceil(segment.duration())).max().orElse(2);
        StringBuilder content=new StringBuilder("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-INDEPENDENT-SEGMENTS\n#EXT-X-TARGETDURATION:"+duration+"\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-PLAYLIST-TYPE:EVENT\n");
        for(var segment:segments)content.append("#EXTINF:").append(segment.duration()).append(",\n").append(segment.filename()).append('\n');
        if(ended)content.append("#EXT-X-ENDLIST\n");
        objects.put(folder.resolve("index.m3u8"),content.toString());
    }
    private void writeSidecar(Path path,Authorized auth) throws Exception {
        access.require(auth.context,auth.resourceId,"ingest");
        Path pending=Path.of(path+".tmp");
        Files.writeString(pending,new JsonObject().put("entityId",auth.entityId).put("security",MediaProvenance.labels(auth.context,auth.entityId)).encode(),StandardOpenOption.CREATE_NEW);
        forceFile(pending); Files.move(pending,path,StandardCopyOption.ATOMIC_MOVE); forceDirectory(path.getParent());
        if(objects!=null)objects.put(path);
    }
    private void session(HttpServerRequest request) {
        String authorization=request.getHeader("Authorization");
        SecurityContext context=access.authenticate(authorization,request.getHeader("X-Workspace-Id"));
        if(authorization.length()>3000) throw new AuthException(401,"Token exceeds cookie session size");
        String value=Base64.getUrlEncoder().withoutPadding().encodeToString(new JsonObject().put("authorization",authorization).put("workspaceId",context.workspaceId()).encode().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        long age=context.expiresAtEpochSecond()-Instant.now().getEpochSecond();
        String cookie="cg_media_session="+value+"; Path=/media/; Max-Age="+age+"; HttpOnly; SameSite=Strict"+("true".equals(System.getenv("SECURITY_ALLOW_HTTP"))?"":"; Secure");
        vertx.runOnContext(v -> request.response().putHeader("Set-Cookie",cookie).putHeader("Cache-Control","private, no-store").end(new JsonObject().put("expiresAt",context.expiresAtEpochSecond()).encode()));
    }
    private void serveMedia(HttpServerRequest request) throws Exception {
        Path file=media.resolve(request.path().substring(7)).normalize();
        if(!file.startsWith(media) || !request.path().matches(".*\\.(m3u8|ts|png|jpg)$")) throw new AuthException(403,"Media unavailable");
        String authorization=request.getHeader("Authorization"),workspace=request.getHeader("X-Workspace-Id");
        if(authorization==null) {
            String cookies=request.getHeader("Cookie");
            if(cookies!=null) for(String cookie:cookies.split(";")) if(cookie.trim().startsWith("cg_media_session=")) {
                try {
                    String value=cookie.trim().substring("cg_media_session=".length());
                    if(value.length()>4096) throw new IllegalArgumentException();
                    JsonObject session=new JsonObject(new String(Base64.getUrlDecoder().decode(value),java.nio.charset.StandardCharsets.UTF_8));
                    authorization=session.getString("authorization"); workspace=session.getString("workspaceId");
                } catch(Exception e) { throw new AuthException(401,"Invalid media session"); }
            }
        }
        SecurityContext context=access.authenticate(authorization,workspace);
        JsonObject labels=(objects==null?MediaProvenance.read(media,file):objects.provenance(file)).getJsonObject("security");
        if(!context.workspaceId().equals(labels.getString("workspaceId"))) throw new AuthException(403,"Permission denied");
        access.require(context,labels.getString("resourceId"),"view");
        long size=objects==null?Files.size(file):objects.size(file),offset=0,length=size; int status=200;
        String range=request.getHeader("Range");
        if(range!=null) {
            if(!range.matches("bytes=[0-9]*-[0-9]*")) throw new AuthException(416,"Invalid range");
            String[] parts=range.substring(6).split("-",-1);
            try {
                if(parts[0].isEmpty()) { length=Math.min(size,Long.parseLong(parts[1])); offset=size-length; }
                else { offset=Long.parseLong(parts[0]); long end=parts[1].isEmpty()?size-1:Math.min(size-1,Long.parseLong(parts[1])); length=end-offset+1; }
                if(offset<0 || offset>=size || length<=0) throw new IllegalArgumentException(); status=206;
            } catch(Exception e) { throw new AuthException(416,"Invalid range"); }
        }
        String type=file.toString().endsWith(".m3u8")?"application/vnd.apple.mpegurl":file.toString().endsWith(".ts")?"video/mp2t":file.toString().endsWith(".png")?"image/png":"image/jpeg";
        final long start=offset,count=length; final int code=status;
        var response=request.response().setStatusCode(code).putHeader("Content-Type",type).putHeader("Cache-Control","private, no-store").putHeader("Vary","Authorization, Cookie, X-Workspace-Id").putHeader("Accept-Ranges","bytes");
        if(code==206) response.putHeader("Content-Range","bytes "+start+"-"+(start+count-1)+"/"+size);
        response.putHeader("Content-Length",Long.toString(count));
        if(request.method()==HttpMethod.HEAD) { response.end(); return; }
        AtomicBoolean stopped=new AtomicBoolean(),checking=new AtomicBoolean();
        String resource=labels.getString("resourceId");
        long expiryTimer=vertx.setTimer(Math.max(1,(context.expiresAtEpochSecond()*1000)-System.currentTimeMillis()),t -> { stopped.set(true); request.connection().close(); });
        long timer=vertx.setPeriodic(1000,t -> {
            if(stopped.get() || !checking.compareAndSet(false,true)) return;
            workers.submit(() -> { try { access.require(context,resource,"view"); }
                catch(Exception e) { stopped.set(true); request.connection().close(); } finally { checking.set(false); } });
        });
        try(InputStream input=objects==null?Files.newInputStream(file.toRealPath(),LinkOption.NOFOLLOW_LINKS):objects.open(file,start,count)) {
            if(objects==null)input.skipNBytes(start);long remaining=count;byte[] buffer=new byte[64*1024];
            while(remaining>0) {
                if(stopped.get() || context.expiresAtEpochSecond()<=Instant.now().getEpochSecond()) throw new AuthException(401,"Media session expired or revoked");
                int n=input.read(buffer,0,(int)Math.min(buffer.length,remaining));
                if(n<0) throw new IOException("Media ended unexpectedly");
                remaining-=n;
                response.write(io.vertx.core.buffer.Buffer.buffer(Arrays.copyOf(buffer,n))).toCompletionStage().toCompletableFuture().get(10,TimeUnit.SECONDS);
            }
            if(!stopped.get()) response.end();
        } catch(Exception e) { request.connection().close(); throw e; }
        finally { stopped.set(true); vertx.cancelTimer(timer); vertx.cancelTimer(expiryTimer); }
    }
    private void reject(HttpServerRequest request,Throwable error) {
        int status=error instanceof AuthException a?a.status():400;
        respond(request,status,new JsonObject().put("status","error").put("message",error instanceof AuthException?error.getMessage():"Invalid request"));
    }
    private void respond(HttpServerRequest request, int code, JsonObject body) {
        vertx.runOnContext(v -> { if (!request.response().ended()) { if (!request.response().headWritten()) request.response().setStatusCode(code).putHeader("Content-Type","application/json").putHeader("Cache-Control","private, no-store"); request.response().end(body.encode()+"\n"); } });
    }
    private void failure(HttpServerRequest request, Endpoint endpoint, Exception exception,Authorized auth) {
        Throwable cause=exception; while(cause.getCause()!=null) cause=cause.getCause();
        if(cause instanceof AuthException || auth==null) { reject(request,cause); return; }
        try { access.require(auth.context,auth.resourceId,"ingest"); } catch(AuthException e) { reject(request,e); return; }
        JsonObject record=new JsonObject().put("errorId",UUID.randomUUID().toString()).put("endpoint",endpoint.name).put("occurredAt",Instant.now().toString()).put("message","Authenticated ingestion processing failure: "+cause.getClass().getSimpleName());
        boolean queued=false;
        try { error.validate(record); producer.send(new ProducerRecord<>(config.getString("errorTopic"),record.getString("errorId"),record.encode())).get(35,TimeUnit.SECONDS); queued=true; }
        catch(Exception e) { System.err.println("Failed to publish error: "+e.getMessage()); }
        respond(request,queued && cause instanceof IllegalArgumentException ? 400 : 503,new JsonObject().put("status","error").put("error",record).put("errorQueued",queued));
    }
    private void stop() {
        draining.set(true);
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(25);
        while(permits.availablePermits()<config.getInteger("maxConcurrentUploads",8) && System.nanoTime()<deadline) { try { Thread.sleep(100); } catch(InterruptedException e) { Thread.currentThread().interrupt(); break; } }
        try { if(server!=null) server.close().toCompletionStage().toCompletableFuture().get(5,TimeUnit.SECONDS); } catch(Exception ignored) {}
        workers.shutdownNow(); if(objects!=null)objects.close(); producer.close(java.time.Duration.ofSeconds(5)); vertx.close();
    }
}
