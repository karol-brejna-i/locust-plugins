import json
from queue import Queue

from elasticsearch import Elasticsearch, exceptions

from locust_plugins.listeners import BaseListener


class ForwardingListener(BaseListener):
    """
    Generic listener that forwards the result to a list of backends (stdout, elasticsearch, ...).
    """
    def __init__(self):
        super().__init__()
        self.backends = set()
        self.quit = False
        self.forwarder_queue = Queue()

    def add_backend(self, backend_adapter):
        self.backends.add(backend_adapter)
        print(f"backend after addition: {self.backends}")

    def remove_backend(self, backend_adapter):
        self.backends.remove(backend_adapter)
        print(f"backend after removal: {self.backends}")

    def request_success(self, request_type, name, response_time, response_length, **kwargs):
        # build payload
        OK_TEMPLATE = '{"request_type":"%s", "name":"%s", "response_time":%s, "response_length":%s, "other":%s}'
        json_string = OK_TEMPLATE % (request_type, name, response_time, response_length, json.dumps(kwargs))
        message = {"type": "success", "payload": json.loads(json_string)}

        # queue the result
        self.add(message)

    def request_failure(self, request_type, name, response_time, exception, **kwargs):
        # build payload
        ERR_TEMPLATE = '{"request_type":"%s", "name":"%s", "response_time":%s, "exception":"%s", "other":%s}'
        json_string = ERR_TEMPLATE % (request_type, name, "ERR", response_time, exception, json.dumps(kwargs))
        message = {"type": "failure", "payload": json.loads(json_string)}

        # queue the result
        self.add(message)

    def add(self, data):
        """
        Add data to be sent.
        :param data:
        :return:
        """
        print("adding to internal buffer")
        self.forwarder_queue.put(data)

    def run(self):
        """
        Listener's event loop: wait for new data and forward it to defined backends.
        :return:
        """
        print("Started forwarder run loop...")
        # forwarder pops a value and uses a list of adapters to forward it to different sinks
        while not self.quit:
            data = self.forwarder_queue.get()
            for backend in self.backends:
                print(f"db forwarder {backend} sending {data}")
                if data["type"] == "failure":
                    backend.handle_failure(data["payload"])
                else:
                    backend.handle_success(data["payload"])


class AdapterError(Exception):
    pass


###########
# Base for concrete adapters
#
class BackendAdapter:

    def id(self):
        return f"{type(self)}"

    def handle_success(self, data):
        pass

    def handle_failure(self, data):
        pass

    def __hash__(self):
        return hash(self.id())


class PrintAdapter(BackendAdapter):
    """
    Print every response (useful when debugging a single locust)
    """

    def __init__(self):
        super().__init__()
        print("type\tname\ttime\tlength\tsuccess\texception")

    def handle_success(self, data):
        self.request_success(**data)

    def handle_failure(self, data):
        self.request_success(**data)

    def request_success(self, request_type, name, response_time, response_length, **_kwargs):
        self._log_request(request_type, name, response_time, response_length, True, None)

    def request_failure(self, request_type, name, response_time, response_length, exception, **_kwargs):
        self._log_request(request_type, name, response_time, response_length, False, exception)

    def _log_request(self, request_type, name, response_time, response_length, success, exception):
        print(f"{request_type}\t{name}\t{response_time}\t{response_length}\t{success}\t{exception}")


class ElasticSearchAdapter(BackendAdapter):
    def __init__(self, elastic_hosts=["http://localhost:9200"], index_name="locust", verify_connection=True):
        self.es = Elasticsearch(elastic_hosts)
        self.index_name = index_name
        if verify_connection:
            import datetime
            try:
                print(f"+++++ {datetime.datetime.now()} -- checking the connection")
                print(self.es.cluster.health())
                print(datetime.datetime.now())
            except exceptions.ConnectionError as ce:
                print("connection error")
                raise AdapterError from ce
            except:
                import sys
                print("---------Unexpected error:", sys.exc_info()[0])
                raise AdapterError

    def handle_success(self, data):
        self.send({"type": "success", "payload": data})

    def handle_failure(self, data):
        self.send({"type": "failure", "payload": data})

    def send(self, data):
        print(f"Sending data {data}")
        # convert data to proper format
        pass

        # store the data in Elasticsearch
        res = self.es.index(index=self.index_name, body=data)
        print(res['result'])
