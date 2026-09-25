package io.contextgraph.query;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import graphql.*;
import io.contextgraph.security.*;
import graphql.scalars.ExtendedScalars;
import graphql.schema.idl.*;
import io.vertx.core.*;
import io.vertx.core.http.*;
import io.vertx.core.json.*;
import io.vertx.ext.web.*;
import io.vertx.ext.web.handler.BodyHandler;
import org.reactivestreams.*;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public final class QueryService {
  static { io.vertx.core.json.jackson.DatabindCodec.mapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS); }
  private final Vertx vertx=Vertx.vertx();
  private HttpServer server;
  private final Set<ServerWebSocket> sockets=ConcurrentHashMap.newKeySet();
  private final ThreadPoolExecutor workers;
  private final ScheduledExecutorService refresh=Executors.newSingleThreadScheduledExecutor();
  private final IcebergQueries db;
  private final AccessControl access=AccessControl.fromEnvironment();
  private final PermissionChecks permissions=PermissionChecks.using(access);
  private final IcebergQueries.Limits limits;
  private final reactor.core.scheduler.Scheduler authScheduler;
  private final LiveBus live;
  private final Path queryFile;
  private volatile GraphQL graph;
  private volatile boolean draining;
  private volatile boolean schemaHealthy;
  private final int port;
  record Snapshot(GraphQL graph) {}
  public QueryService(JsonNode cfg) throws Exception {
    limits=new IcebergQueries.Limits(cfg.path("maxSourceRows").asInt(100000),cfg.path("maxSourceResources").asInt(10000),cfg.path("duckdbMemoryLimit").asText("64MB"),cfg.path("queryTimeoutSeconds").asInt(30));
    port=cfg.path("port").asInt(8081);queryFile=Path.of(cfg.path("queries").asText("config/queries.yaml"));
    db=new IcebergQueries(Path.of(System.getenv().getOrDefault("WAREHOUSE",cfg.path("warehouse").asText("/data/warehouse"))));
    int n=cfg.path("workers").asInt(4);workers=new ThreadPoolExecutor(n,n,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(cfg.path("queueCapacity").asInt(64)),new ThreadPoolExecutor.AbortPolicy());
    authScheduler=reactor.core.scheduler.Schedulers.fromExecutor(workers);
    JsonNode definition=yaml(queryFile);Set<String> topics=new HashSet<>();definition.path("subscriptions").elements().forEachRemaining(s->s.path("topics").forEach(t->topics.add(t.asText())));
    live=new LiveBus(System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS",cfg.path("kafkaBootstrap").asText("localhost:9092")),topics,cfg.path("subscriptionBuffer").asInt(256));
    // Fail startup if the requested DuckDB extensions cannot actually load.
    try(var ignored=IcebergQueries.connect(true)) {}
    reload();refresh.scheduleWithFixedDelay(()->{try{reload();}catch(Exception e){schemaHealthy=false;System.err.println("Schema refresh failed: "+e.getMessage());}},15,cfg.path("schemaRefreshSeconds").asInt(15),TimeUnit.SECONDS);
  }
  static JsonNode yaml(Path p)throws Exception{return new ObjectMapper(new YAMLFactory()).readTree(p.toFile());}
  private synchronized void reload() throws Exception {
    JsonNode config=yaml(queryFile);
    Map<String,IcebergQueries.Policy> policies=new HashMap<>();
    config.path("registeredTables").fields().forEachRemaining(e->{
      if(!e.getKey().matches("context_secure(?:_[a-z0-9_]+)?\\.[a-z_][a-z0-9_]*"))throw new SecurityException("Only secure tables may be registered");
      policies.put(e.getKey(),new IcebergQueries.Policy(e.getValue().path("entityColumn").asText(),e.getValue().has("targetEntityColumn")?e.getValue().path("targetEntityColumn").asText():null));
    });
    Map<String,IcebergQueries.Table> tables=new HashMap<>();
    db.discover().forEach((name,table)->{if(IcebergQueries.registered(table,policies.get(name)))tables.put(name,table);});
    StringBuilder sdl=new StringBuilder(config.path("types").asText());
    sdl.append("\ntype Column { name: String! type: String! nullable: Boolean! }\ntype TableSchema { name: String! columns: [Column!]! }\ntype Query { schemas: [TableSchema!]!\n");
    RuntimeWiring.Builder wiring=RuntimeWiring.newRuntimeWiring().scalar(ExtendedScalars.Json).scalar(ExtendedScalars.GraphQLLong);
    TypeRuntimeWiring.Builder query=TypeRuntimeWiring.newTypeWiring("Query").dataFetcher("schemas",env->CompletableFuture.supplyAsync(()->{
      SecurityContext context=env.getGraphQlContext().get("security");permissions.workspace(context,"view_schema");
      return tables.values().stream().map(t->Map.of("name",t.name(),"columns",t.columns().stream().map(c->Map.of("name",c.name(),"type",c.type(),"nullable",c.nullable())).toList())).toList();
    },workers));
    config.path("queries").fields().forEachRemaining(entry->{
      JsonNode q=entry.getValue();Map<String,IcebergQueries.Source> sources=new LinkedHashMap<>();
      if(q.has("sources"))q.path("sources").fields().forEachRemaining(e->{String name=e.getValue().asText();if(!policies.containsKey(name))throw new SecurityException("Unregistered federated table");sources.put(e.getKey(),new IcebergQueries.Source(tables.get(name),policies.get(name)));});
      else {String name=q.path("table").asText();if(!policies.containsKey(name))throw new SecurityException("Unregistered query table");sources.put("source",new IcebergQueries.Source(tables.get(name),policies.get(name)));}
      IcebergQueries.validateSources(sources.keySet());SafeSql.validate(q.path("sql").asText(),sources.keySet());
      sdl.append(q.path("signature").asText()).append('\n');
      query.dataFetcher(entry.getKey(),env->CompletableFuture.supplyAsync(()->{try{
        List<Map<String,Object>> rows;
        if(q.has("sources"))rows=db.federate(sources,q.path("sql").asText(),env.getArguments(),env.getGraphQlContext().get("security"),permissions,limits);
        else {var source=sources.get("source");
        rows=db.execute(source.table(),source.policy(),q.path("sql").asText(),env.getArguments(),env.getGraphQlContext().get("security"),permissions,limits);}
        if(q.path("engine").asText().equals("bm25")||q.path("engine").asText().equals("documents"))return DocumentSearch.execute(rows,env.getArguments(),q.path("engine").asText().equals("documents"),env.getGraphQlContext().get("security"),permissions,q.path("ranking"));
        return rows;
      }catch(Exception e){throw new CompletionException(e);}},workers));
    });
    sdl.append("}\n");if(config.path("subscriptions").size()>0)sdl.append("type Subscription {\n");TypeRuntimeWiring.Builder subscriptions=TypeRuntimeWiring.newTypeWiring("Subscription");
    config.path("subscriptions").fields().forEachRemaining(entry->{JsonNode q=entry.getValue();sdl.append(q.path("signature").asText()).append('\n');List<String> topics=new ArrayList<>();q.path("topics").forEach(t->topics.add(t.asText()));subscriptions.dataFetcher(entry.getKey(),env->CompletableFuture.supplyAsync(()->{
      if(!live.ready())throw new IllegalStateException("Subscriptions unavailable");
      SecurityContext context=env.getGraphQlContext().get("security");permissions.workspace(context,"access");String entity=env.getArgument("entityId");
      if(entity!=null)permissions.require(context,AccessControl.resourceId(context.workspaceId(),entity));
      return live.stream(topics,q.path("entityField").asText("entityId"),entity,q.path("wrap").asBoolean(),context,permissions,authScheduler);
    },workers));});
    if(config.path("subscriptions").size()>0){sdl.append("}\n");wiring.type(subscriptions);}
    if(config.path("mutations").size()>0) {
      sdl.append("type Mutation {\n");var mutations=TypeRuntimeWiring.newTypeWiring("Mutation");
      config.path("mutations").fields().forEachRemaining(e->{var definition=e.getValue();sdl.append(definition.path("signature").asText()).append('\n');
        mutations.dataFetcher(e.getKey(),env->CompletableFuture.supplyAsync(()->{try{return EventMutations.publish(definition.path("path").asText(),env.getArgument("input"),env.getArgument("resourceKey"),env.getGraphQlContext().get("authorization"),env.getGraphQlContext().get("security"),access);}catch(Exception error){throw new CompletionException(error);}},workers));});
      sdl.append("}\n");wiring.type(mutations);
    }
    wiring.type(query);graph=GraphQL.newGraphQL(new SchemaGenerator().makeExecutableSchema(new SchemaParser().parse(sdl.toString()),wiring.build())).defaultDataFetcherExceptionHandler(parameters->CompletableFuture.completedFuture(graphql.execution.DataFetcherExceptionHandlerResult.newResult().error(GraphqlErrorBuilder.newError(parameters.getDataFetchingEnvironment()).message("Request denied or query unavailable").build()).build())).instrumentation(new graphql.execution.instrumentation.ChainedInstrumentation(List.of(new graphql.analysis.MaxQueryDepthInstrumentation(12),new graphql.analysis.MaxQueryComplexityInstrumentation(1000)))).build();schemaHealthy=true;
  }
  static String graphType(String type){return switch(type){case "int"->"Int";case "long"->"Long";case "float","double"->"Float";case "boolean"->"Boolean";case "variant"->"JSON";default->type.startsWith("{")?"JSON":"String";};}
  ExecutionInput input(JsonObject body,SecurityContext context){return ExecutionInput.newExecutionInput().query(body.getString("query", "")).operationName(body.getString("operationName")).variables(body.getJsonObject("variables",new JsonObject()).getMap()).graphQLContext(Map.of("security",context)).build();}
  public void start(){
    HttpServerOptions httpOptions=serverOptions();
    HttpMetrics.start();
    Router router=Router.router(vertx);router.route().handler(c->{long began=System.nanoTime();c.addBodyEndHandler(v->HttpMetrics.observe(HttpMetrics.route(c.request().path()),c.response().getStatusCode(),began));c.next();});router.get("/health/live").handler(c->c.response().end("ok"));router.get("/health/ready").handler(c->c.response().setStatusCode(!draining&&schemaHealthy?200:503).end());
    router.get("/health/subscriptions").handler(c->c.response().setStatusCode(!draining&&schemaHealthy&&live.ready()?200:503).end());
    router.post("/graphql").handler(c->{c.request().pause();
      try {CompletableFuture.supplyAsync(()->access.authenticate(c.request().getHeader("Authorization"),c.request().getHeader("X-Workspace-Id")),workers).whenComplete((context,error)->vertx.runOnContext(v->{if(error!=null){c.response().putHeader("Connection","close").setStatusCode(authStatus(error)).end("Authentication or authorization failed");return;}c.put("security",context);c.next();c.request().resume();}));}
      catch(RejectedExecutionException e){c.response().setStatusCode(503).end();}
    });
    router.post("/graphql").handler(BodyHandler.create().setBodyLimit(64*1024)).handler(c->{try{graph.executeAsync(input(c.body().asJsonObject(),c.get("security")).transform(b->b.graphQLContext(Map.of("authorization",c.request().getHeader("Authorization"))))).whenComplete((result,error)->vertx.runOnContext(v->{if(error!=null)c.response().setStatusCode(500).end();else if(result.getData() instanceof Publisher<?>)c.response().setStatusCode(400).end("Use graphql-transport-ws for subscriptions");else {if(!result.getErrors().isEmpty())HttpMetrics.event("graphql_error");c.response().putHeader("content-type","application/json").end(Json.encode(result.toSpecification()));}}));}catch(Exception e){c.response().setStatusCode(400).end();}});
    server=vertx.createHttpServer(httpOptions).webSocketHandler(this::socket).requestHandler(router);
    server.listen(port).onFailure(e->{e.printStackTrace();System.exit(1);});
    Runtime.getRuntime().addShutdownHook(new Thread(this::close));
  }
  private static HttpServerOptions serverOptions() {
    var options=new HttpServerOptions().setIdleTimeout(60).setMaxWebSocketMessageSize(64*1024).setMaxWebSocketFrameSize(64*1024).setWebSocketSubProtocols(List.of("graphql-transport-ws"));
    String path=System.getenv("TLS_KEYSTORE_PATH"),password=System.getenv("TLS_KEYSTORE_PASSWORD");
    if(path!=null&&!path.isBlank()&&password!=null&&!password.isBlank())options.setSsl(true).setKeyCertOptions(new io.vertx.core.net.PfxOptions().setPath(path).setPassword(password));
    else if(!"true".equals(System.getenv("SECURITY_ALLOW_HTTP")))throw new IllegalStateException("TLS server credentials are required");
    return options;
  }
  private void socket(ServerWebSocket ws){
    if(draining||!ws.path().equals("/graphql")||!"graphql-transport-ws".equals(ws.subProtocol())){ws.close((short)4406,"Unsupported protocol");return;}
    sockets.add(ws);Map<String,Disposable> operations=new ConcurrentHashMap<>();Map<String,Object> pending=new ConcurrentHashMap<>();boolean[] initialized={false},initializing={false};SecurityContext[] session={null};String[] authorization={null};long[] expiryTimer={-1};
    long timeout=vertx.setTimer(5000,id->{if(!initialized[0])ws.close((short)4408,"Initialization timeout");});
    ws.setWriteQueueMaxSize(256*1024);
    ws.textMessageHandler(text->{try{JsonObject message=new JsonObject(text);String id=message.getString("id"),type=message.getString("type");switch(type){
      case "connection_init"->{if(initialized[0]||initializing[0]){ws.close((short)4429,"Already initialized");return;}initializing[0]=true;
        JsonObject payload=message.getJsonObject("payload",new JsonObject());
        CompletableFuture.supplyAsync(()->access.authenticate(payload.getString("authorization"),payload.getString("workspaceId")),workers).whenComplete((context,error)->vertx.runOnContext(v->{
          if(ws.isClosed())return;if(error!=null){ws.close((short)4401,"Authentication failed");return;}session[0]=context;authorization[0]=payload.getString("authorization");initialized[0]=true;
          long delay=Math.max(1,context.expiresAtEpochSecond()*1000-System.currentTimeMillis());expiryTimer[0]=vertx.setTimer(delay,t->ws.close((short)4401,"Session expired"));
          send(ws,new JsonObject().put("type","connection_ack"));
        }));}
      case "ping"->send(ws,new JsonObject().put("type","pong").put("payload",message.getValue("payload")));
      case "pong"->{}
      case "complete"->{pending.remove(id);Disposable d=operations.remove(id);if(d!=null)d.dispose();}
      case "subscribe"->{if(!initialized[0]){ws.close((short)4401,"Unauthorized");return;}if(id==null||id.isBlank()){ws.close((short)4400,"Operation id required");return;}Object token=new Object();if(pending.putIfAbsent(id,token)!=null){ws.close((short)4409,"Duplicate operation");return;}if(pending.size()>64){ws.close((short)4429,"Too many operations");return;}
        CompletableFuture.supplyAsync(()->{permissions.workspace(session[0],"access");return input(message.getJsonObject("payload"),session[0]).transform(b->b.graphQLContext(Map.of("authorization",authorization[0])));},workers).thenCompose(graph::executeAsync).whenComplete((result,error)->vertx.runOnContext(v->{if(ws.isClosed()||pending.get(id)!=token)return;if(error!=null||!result.getErrors().isEmpty()){send(ws,new JsonObject().put("id",id).put("type","error").put("payload",error!=null?List.of(Map.of("message","Execution failed")):result.getErrors().stream().map(GraphQLError::toSpecification).toList()));pending.remove(id,token);return;}
          if(result.getData() instanceof Publisher<?> publisher){Disposable d=Flux.from(publisher).subscribe(item->send(ws,new JsonObject().put("id",id).put("type","next").put("payload",((ExecutionResult)item).toSpecification())),err->{send(ws,new JsonObject().put("id",id).put("type","error").put("payload",List.of(Map.of("message","Subscription overflow or failure"))));pending.remove(id,token);operations.remove(id);},()->{send(ws,new JsonObject().put("id",id).put("type","complete"));pending.remove(id,token);operations.remove(id);});operations.put(id,d);}else {send(ws,new JsonObject().put("id",id).put("type","next").put("payload",result.toSpecification()));send(ws,new JsonObject().put("id",id).put("type","complete"));pending.remove(id,token);}
        }));}
      default->ws.close((short)4400,"Invalid message");
    }}catch(Exception e){ws.close((short)4400,"Invalid message");}});
    ws.closeHandler(v->{vertx.cancelTimer(timeout);if(expiryTimer[0]>=0)vertx.cancelTimer(expiryTimer[0]);operations.values().forEach(Disposable::dispose);operations.clear();pending.clear();sockets.remove(ws);});
  }
  private static int authStatus(Throwable error){while(error.getCause()!=null)error=error.getCause();return error instanceof AuthException a?a.status():503;}
  private void send(ServerWebSocket ws,JsonObject message){if(ws.isClosed())return;if(ws.writeQueueFull()){ws.close((short)1013,"Slow consumer");return;}ws.writeTextMessage(message.encode()).onFailure(e->ws.close());}
  private void close(){
    draining=true;
    refresh.shutdownNow();
    sockets.forEach(s->s.close((short)1012,"Service restart; reconnect"));
    // Keep workers and the event loop alive through authentication, query execution,
    // and response flush. Worker termination alone does not await HTTP callbacks.
    try {if(server!=null)drainHttp(server).toCompletableFuture().get(42,TimeUnit.SECONDS);}
    catch(Exception e){System.err.println("HTTP drain deadline or failure: "+e.getClass().getSimpleName());}
    live.close();
    authScheduler.dispose();
    workers.shutdown();
    try {if(!workers.awaitTermination(5,TimeUnit.SECONDS))workers.shutdownNow();}
    catch(InterruptedException e){workers.shutdownNow();Thread.currentThread().interrupt();}
    try {vertx.close().toCompletionStage().toCompletableFuture().get(5,TimeUnit.SECONDS);}
    catch(Exception e){System.err.println("Vert.x close deadline or failure: "+e.getClass().getSimpleName());}
  }
  static CompletionStage<Void> drainHttp(HttpServer server){return server.shutdown(40,TimeUnit.SECONDS).toCompletionStage();}
  public static void main(String[] args){try{new QueryService(yaml(Path.of(System.getenv().getOrDefault("QUERY_CONFIG",args.length>0?args[0]:"config/query.yaml")))).start();}catch(Exception e){System.err.println("Query startup failed: "+e.getClass().getSimpleName()+": "+e.getMessage());System.exit(1);}}
}
