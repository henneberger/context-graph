"""Payload-free HTTP metrics for standard-library services, on an internal listener."""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import threading,time,collections,resource
_lock=threading.Lock();_counts=collections.Counter();_duration=collections.Counter();_start=time.monotonic()
def instrument(handler):
    original=handler.send_response
    def response(self,code,message=None):
        with _lock:
            _counts[str(code)]+=1
            _duration[str(code)]+=max(0,time.monotonic()-getattr(self,'_metrics_begin',time.monotonic()))
        return original(self,code,message)
    handler.send_response=response
    request=handler.handle_one_request
    def handle(self):self._metrics_begin=time.monotonic();return request(self)
    handler.handle_one_request=handle
    class Metrics(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            if self.path!='/metrics':self.send_error(404);return
            with _lock:
                lines=['# TYPE context_http_requests_total counter']+[f'context_http_requests_total{{status="{k}",route="service"}} {v}' for k,v in _counts.items()]
                lines+=['# TYPE context_http_response_duration_seconds summary']+[f'context_http_response_duration_seconds_sum{{status="{k}"}} {v}' for k,v in _duration.items()]+[f'context_http_response_duration_seconds_count{{status="{k}"}} {v}' for k,v in _counts.items()]
            lines += [f'context_process_uptime_seconds {time.monotonic()-_start}',f'context_process_max_resident_memory_bytes {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024}']
            data=('\n'.join(lines)+'\n').encode();self.send_response(200);self.send_header('Content-Type','text/plain; version=0.0.4');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    server=ThreadingHTTPServer(('0.0.0.0',9404),Metrics);threading.Thread(target=server.serve_forever,daemon=True).start()
