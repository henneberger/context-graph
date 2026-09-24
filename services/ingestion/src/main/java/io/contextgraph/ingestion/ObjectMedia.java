package io.contextgraph.ingestion;

import io.contextgraph.security.*;
import io.vertx.core.json.JsonObject;
import java.io.*;
import java.net.URI;
import java.nio.file.*;
import java.time.Duration;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;

/** Private S3 media storage; all delivery continues through the SpiceDB-enforcing API. */
final class ObjectMedia implements AutoCloseable {
  private final S3Client s3;
  private final String bucket;
  private final Path root;
  ObjectMedia(Path root){
    this.root=root;bucket=System.getenv().getOrDefault("MEDIA_S3_BUCKET","context-media");String endpoint=System.getenv("MEDIA_S3_ENDPOINT");
    if(endpoint==null||!endpoint.startsWith("https://"))throw new IllegalArgumentException("Media object storage requires TLS");
    s3=S3Client.builder().endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1).forcePathStyle(true)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(System.getenv("MEDIA_S3_ACCESS_KEY"),System.getenv("MEDIA_S3_SECRET_KEY"))))
      .overrideConfiguration(b->b.apiCallTimeout(Duration.ofSeconds(30)).apiCallAttemptTimeout(Duration.ofSeconds(15))).build();
  }
  String key(Path file){Path normalized=file.toAbsolutePath().normalize();if(!normalized.startsWith(root))throw new AuthException(403,"Invalid object path");String key=root.relativize(normalized).toString();if(key.isBlank()||key.contains("\\"))throw new AuthException(403,"Invalid object path");return key;}
  void put(Path file){s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key(file)).build(),RequestBody.fromFile(file));}
  void put(Path file,String content){s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key(file)).build(),RequestBody.fromString(content));}
  JsonObject provenance(Path file)throws Exception {
    Path sidecar=MediaProvenance.sidecar(root,file);
    try(var in=s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key(sidecar)).build())) {
      byte[] bytes=in.readNBytes(8193);if(bytes.length>8192)throw new AuthException(403,"Media unavailable");
      JsonObject data=new JsonObject(new String(bytes,java.nio.charset.StandardCharsets.UTF_8));MediaProvenance.validate(data);return data;
    }catch(S3Exception e){throw new AuthException(e.statusCode()==404?403:503,"Media unavailable");}
  }
  long size(Path file){return s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key(file)).build()).contentLength();}
  InputStream open(Path file,long offset,long length){return s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key(file)).range("bytes="+offset+"-"+(offset+length-1)).build());}
  public void close(){s3.close();}
}
