#!/usr/bin/env python3
"""Exercise denied Kafka operations with actual application credentials."""
import argparse,json,subprocess,time
p=argparse.ArgumentParser();p.add_argument('--context',required=True);a=p.parse_args()
k=['kubectl','--context',a.context,'-n','context-graph']
cases={
 'query':('printf \'{}\\n\' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server secure-kafka:9092 --topic cg.secure.events --producer.config /app/secrets/kafka.properties --producer-property max.block.ms=10000 --producer-property delivery.timeout.ms=10000 --producer-property request.timeout.ms=5000','TopicAuthorizationException'),
 'ingestion':('/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server secure-kafka:9092 --topic cg.secure.metrics --group unauthorized-ingestion-probe --consumer.config /app/secrets/kafka.properties --timeout-ms 10000 --max-messages 1','AuthorizationException')}
for role,(cmd,expected) in cases.items():
 name='acl-denial-'+role
 subprocess.run(k+['delete','pod',name,'--ignore-not-found','--wait=true'],check=True,stdout=subprocess.DEVNULL)
 pod={'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'labels':{'app':'security-acl-probe','context-graph-role':role}},'spec':{'restartPolicy':'Never','automountServiceAccountToken':False,'securityContext':{'runAsUser':1000,'runAsGroup':2000,'fsGroup':2000},'containers':[{'name':'probe','image':'apache/kafka:4.1.1','command':['sh','-c',cmd],'resources':{'requests':{'memory':'128Mi'},'limits':{'memory':'256Mi'}},'env':[{'name':'KAFKA_HEAP_OPTS','value':'-Xmx128m -Xms64m'}],'volumeMounts':[{'name':'credentials','mountPath':'/app/secrets','readOnly':True}]}],'volumes':[{'name':'credentials','secret':{'secretName':'security-'+role,'defaultMode':288}}]}}
 subprocess.run(k+['apply','-f','-'],input=json.dumps(pod),text=True,check=True,stdout=subprocess.DEVNULL)
 deadline=time.monotonic()+120
 while time.monotonic()<deadline:
  phase=subprocess.check_output(k+['get','pod',name,'-o','jsonpath={.status.phase}'],text=True)
  if phase in ('Succeeded','Failed'):break
  time.sleep(2)
 logs=subprocess.check_output(k+['logs',name],text=True,stderr=subprocess.STDOUT)
 if expected not in logs:raise RuntimeError(f'{role}: expected explicit authorization denial, got phase {phase}; inspect pod logs')
 print(f'PASS: {role} unauthorized Kafka operation denied')
 subprocess.run(k+['delete','pod',name,'--wait=false'],check=True,stdout=subprocess.DEVNULL)
