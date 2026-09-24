package io.contextgraph.query;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;
import java.net.URI;
import java.net.http.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class HttpDrainTest {
  @Test void shutdownWaitsForAcceptedAsyncResponseAndClosesListener() throws Exception {
    var vertx=Vertx.vertx();
    var workers=Executors.newSingleThreadExecutor();
    var accepted=new CompletableFuture<Void>();
    var release=new CompletableFuture<Void>();
    var server=vertx.createHttpServer().requestHandler(request->{
      accepted.complete(null);
      release.thenApplyAsync(v->"authorized result",workers).whenComplete((body,error)->
          vertx.runOnContext(v->request.response().end(body)));
    }).listen(0,"127.0.0.1").toCompletionStage().toCompletableFuture().get(5,TimeUnit.SECONDS);
    try(var client=HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
      var uri=URI.create("http://127.0.0.1:"+server.actualPort()+"/graphql");
      var response=client.sendAsync(HttpRequest.newBuilder(uri).POST(HttpRequest.BodyPublishers.ofString("query")).build(),HttpResponse.BodyHandlers.ofString());
      accepted.get(5,TimeUnit.SECONDS);
      var drained=QueryService.drainHttp(server).toCompletableFuture();
      assertFalse(drained.isDone(),"Shutdown must wait for the accepted response");
      release.complete(null);
      assertEquals("authorized result",response.get(5,TimeUnit.SECONDS).body());
      drained.get(5,TimeUnit.SECONDS);
      assertThrows(Exception.class,()->client.send(HttpRequest.newBuilder(uri).GET().timeout(java.time.Duration.ofSeconds(2)).build(),HttpResponse.BodyHandlers.ofString()));
    } finally {
      release.complete(null);
      workers.shutdownNow();
      vertx.close().toCompletionStage().toCompletableFuture().get(5,TimeUnit.SECONDS);
    }
  }
}
