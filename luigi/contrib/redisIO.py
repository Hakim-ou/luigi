import redis
import io
import json
from luigi.format import Format, TextFormat

redis_client = redis.Redis(host='localhost', port=6379, db=0)

class CacheExecuter:
    def __init__(self, output_file):
        self.output_file = output_file

    def __getattribute__(self, name):
        if name == "close":
            path = self.output_file.name
            getattr(self.output_file, "close")
            cache_file(path)
        else:
            getattr(self.output_file, name)

#class CacheFormat(TextFormat):
class CacheFormat(Format):
    def __init__(self, format_class, *args, **kwargs):
        """
        format_class is the Format we want to use along side the cache
        """
        module = __import__("luigi.format")
        class_ = getattr(module, format_class)
        if len(args) != 0 and len(kwargs) != 0:
            self.format = class_(*args, **kwargs)
        elif len(args) != 0:
            self.format = class_(*args)
        elif len(kwargs) != 0:
            self.format = class_(**kwargs)
        else:
            self.format = class_()
    
    def pipe_reader(self, input_pipe):
        # not sure that input_pipe.name will return the path.
        # If not we can use a global map to link input_files to their paths.
        stream = hit_cache(input_pipe.name)
        if stream is None: 
            return self.format.pipe_reader(input_pipe)
        return stream

    def pipe_writer(self, output_pipe):
        return self.format.pipe_writer(CacheExecuter(output_pipe))


def cache_file(path):
    lines = []
    with open(path, "r") as f:
        lines = f.readlines()
        lines = json.dumps(lines) 
        redis_client.set(path, lines)

def hit_cache(path):
    lines = redis_client.get(path)
    if lines is not None:
        return io.StringIO("\n".join(json.loads(lines)))
    return None



