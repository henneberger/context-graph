package io.contextgraph.ingestion;

import io.vertx.core.json.JsonObject;
import io.contextgraph.security.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class IngestionTest {
    @TempDir Path temp;
    Path schemas() { return Path.of("../../config/schemas"); }
    @Test void envelopePreservesArbitraryPayloadButRejectsMissingMetadata() throws Exception {
        var validator = new SchemaCheck(schemas().resolve("envelope.json"));
        var e = new JsonObject().put("eventId",UUID.randomUUID().toString()).put("schemaVersion",2).put("endpoint","events").put("kind","json")
                .put("entityId","sensor").put("eventTime",Instant.now().toString()).put("ingestedAt",Instant.now().toString()).put("payload",new JsonObject().put("unknown",List.of(1,2,3))).put("security",MediaProvenance.labels(new SecurityContext("alice","workspace-a",Instant.now().getEpochSecond()+60),"sensor"));
        validator.validate(e);
        e.put("kind","video"); assertThrows(IllegalArgumentException.class,()->validator.validate(e));
        e.put("kind","json").remove("entityId"); assertThrows(IllegalArgumentException.class,()->validator.validate(e));
    }
    @Test void inputRejectsWrongKnownTypesButAllowsUnstructuredObjects() throws Exception {
        var schema=new SchemaCheck(schemas().resolve("event-input.json"));
        assertThrows(IllegalArgumentException.class,()->schema.validate(new JsonObject().put("eventTime","invalid")));
        schema.validate(new JsonObject().put("entityId","sensor").put("nested",new JsonObject().put("a",true)));
        // A payload key named security is ordinary data; authorization comes from the server-stamped envelope.
        schema.validate(new JsonObject().put("security",new JsonObject().put("subjectId","admin")));
        assertThrows(IllegalArgumentException.class,()->schema.validate(new JsonObject().put("value","not numeric")));
    }
    @Test void mediaWithoutSidecarAndRemappedLabelsDeny() throws Exception {
        Path file=temp.resolve("image.png"); Files.write(file,new byte[]{1,2,3});
        assertThrows(AuthException.class,()->MediaProvenance.read(temp,file));
        var labels=MediaProvenance.labels(new SecurityContext("alice","workspace-a",Instant.now().getEpochSecond()+60),"sensor");
        Path sidecar=MediaProvenance.sidecar(temp,file);
        Files.writeString(sidecar,new JsonObject().put("entityId","sensor").put("security",labels).encode());
        assertEquals("workspace-a",MediaProvenance.read(temp,file).getJsonObject("security").getString("workspaceId"));
        labels.put("workspaceId","workspace-b"); Files.writeString(sidecar,new JsonObject().put("entityId","sensor").put("security",labels).encode());
        assertThrows(AuthException.class,()->MediaProvenance.read(temp,file));
    }
    @Test void playlistIgnoresIncompleteSegments() throws Exception {
        Path p=temp.resolve("index.m3u8");
        Files.writeString(p,"#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:3\n#EXTINF:2.0,\nchunk-00000003.ts\n#EXTINF:0.7,\n");
        var segments=VideoSegments.read(p); assertEquals(1,segments.size()); assertEquals(3,segments.getFirst().sequence());
    }
    @Test void imageMagicRejectsPlaylistsAndAcceptsRealPngAndJpeg() throws Exception {
        Path playlist=temp.resolve("upload.png"); Files.writeString(playlist,"#EXTM3U\nfile:///data/media/another-entity.ts\n");
        assertThrows(IllegalArgumentException.class,()->ImageMetadata.requireSupportedSignature(playlist));
        var image=new java.awt.image.BufferedImage(2,2,java.awt.image.BufferedImage.TYPE_INT_RGB);
        for(String format:List.of("png","jpeg")) {
            Path file=temp.resolve("valid."+format); assertTrue(javax.imageio.ImageIO.write(image,format,file.toFile()));
            ImageMetadata.requireSupportedSignature(file);
        }
    }
    @Test void uploadedPlaylistsCannotOpenNetworkConnections() throws Exception {
        boolean available;
        try { available=new ProcessBuilder("ffmpeg","-version").redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD).start().waitFor()==0; }
        catch(Exception e) { available=false; }
        org.junit.jupiter.api.Assumptions.assumeTrue(available,"ffmpeg integration requires installed binary");
        var requests=new java.util.concurrent.atomic.AtomicInteger();
        var server=com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/other.ts",exchange -> { requests.incrementAndGet(); exchange.sendResponseHeaders(200,0); exchange.close(); }); server.start();
        try {
            Path input=temp.resolve("upload.m3u8");
            Files.writeString(input,"#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:2.0,\nhttp://127.0.0.1:"+server.getAddress().getPort()+"/other.ts\n#EXT-X-ENDLIST\n");
            var command=new ArrayList<>(VideoSegments.command(temp,2));
            // Exercise the protocol boundary even on newer FFmpeg builds whose
            // auto-detection already rejects HLS delivered through a pipe.
            int inputIndex=command.indexOf("-i"); command.add(inputIndex,"hls"); command.add(inputIndex,"-f");
            var process=new ProcessBuilder(command).redirectInput(input.toFile()).start();
            String errors=new String(process.getErrorStream().readAllBytes());
            assertTrue(process.waitFor(15,TimeUnit.SECONDS)); assertNotEquals(0,process.exitValue());
            assertTrue(errors.contains("not on whitelist"),errors); assertEquals(0,requests.get());
        } finally { server.stop(0); }
    }
    @Test void uploadedPlaylistsCannotReadOtherLocalMedia() throws Exception {
        boolean available;
        try { available=new ProcessBuilder("ffmpeg","-version").redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD).start().waitFor()==0; }
        catch(Exception e) { available=false; }
        org.junit.jupiter.api.Assumptions.assumeTrue(available,"ffmpeg integration requires installed binary");
        Path hidden=temp.resolve("other-entity.ts"); Files.writeString(hidden,"protected other entity bytes");
        Path input=temp.resolve("upload.m3u8");
        Files.writeString(input,"#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:2.0,\n"+hidden.toUri()+"\n#EXT-X-ENDLIST\n");
        var command=new ArrayList<>(VideoSegments.command(temp,2));
            // Exercise the protocol boundary even on newer FFmpeg builds whose
            // auto-detection already rejects HLS delivered through a pipe.
            int inputIndex=command.indexOf("-i"); command.add(inputIndex,"hls"); command.add(inputIndex,"-f");
            var process=new ProcessBuilder(command).redirectInput(input.toFile()).start();
        String errors=new String(process.getErrorStream().readAllBytes());
        assertTrue(process.waitFor(15,TimeUnit.SECONDS)); assertNotEquals(0,process.exitValue());
        assertTrue(errors.contains("not on whitelist"),errors);
        assertFalse(Files.exists(temp.resolve("chunk-00000000.ts")));
    }
    @Test void ffmpegSegmentsStartWithDecodableKeyframes() throws Exception {
        boolean available;
        try { available=new ProcessBuilder("ffmpeg","-version").redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD).start().waitFor()==0; } catch(Exception e) { available=false; }
        org.junit.jupiter.api.Assumptions.assumeTrue(available,"ffmpeg integration requires installed binary");
        Path input=temp.resolve("source.ts");
        var source=new ProcessBuilder("ffmpeg","-v","error","-f","lavfi","-i","testsrc2=size=160x120:rate=24","-t","5","-c:v","libx264","-g","97","-f","mpegts",input.toString()).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        assertTrue(source.waitFor(30,TimeUnit.SECONDS)); assertEquals(0,source.exitValue());
        var ffmpeg=new ProcessBuilder(VideoSegments.command(temp,2)).redirectInput(input.toFile()).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        assertTrue(ffmpeg.waitFor(30,TimeUnit.SECONDS)); assertEquals(0,ffmpeg.exitValue());
        var segments=VideoSegments.read(temp.resolve("index.m3u8")); assertEquals(3,segments.size());
        for(var segment:segments) {
            var probe=new ProcessBuilder("ffprobe","-v","error","-select_streams","v:0","-show_entries","frame=key_frame","-of","csv=p=0",temp.resolve(segment.filename()).toString()).start();
            String frames=new String(probe.getInputStream().readAllBytes()); assertEquals(0,probe.waitFor()); assertTrue(frames.startsWith("1"),frames);
            assertTrue(segment.duration()<=2.1);
        }
    }
  @org.junit.jupiter.api.Test void arbitraryJsonNumbersKeepTheirPrecisionBeforeKafka() throws Exception {
    String json="{\"value\":12345678901234567890.123456789,\"unknown\":{\"nested\":[null,true,1.25]}}";
    var parsed=IngestionMain.parsePayload(json);
    assertTrue(io.vertx.core.json.Json.encode(parsed).contains("12345678901234567890.123456789"));
    assertEquals(new java.math.BigDecimal("12345678901234567890.123456789"),((java.util.Map<?,?>)parsed).get("value"));
  }
}
