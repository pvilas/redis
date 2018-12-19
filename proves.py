import redis
from loguru import logger
from datetime import datetime
from collections import namedtuple

red=redis.Redis(
    host='localhost',
    decode_responses=True
)
 
class Base:

    def __init__(self, 
                 name='table',
                 fields=()): 
        """ Base for records
            :name: the name of the 'table'
            :fields: tuple with field names used in get
        """                
        super().__init__()    
        self._name=name
        # create named tuple from field names
        self._fields=('id',)+fields
        self._record=namedtuple(name, self._fields)
        # default key format, note that autoincrmental has its own
        self._format_key='{}:{}'

    def fkey(self, k):
        # return name:k
        return self._format_key.format(self._name, k)

    def check_set(self, **kwargs):
        # checks if each field exists on field list
        for k in kwargs.keys():
            if not k in self._fields:
                raise Exception(f'field {k} is not on the {self._name} fieldlist: {self._fields}')

    def get(self, key):
        # get the record
        #logger.debug(f'key is {key} and fkey is {self.fkey(key)}')
        rec=red.hmget(self.fkey(key), self._fields)
        rec[0]=self.fkey(key)
        #logger.debug(f'record is {dir(self._record)}')
        #logger.debug(f'rec is {rec}')
        return self._record(*rec)

    def filter(self, match='*'):
        # return a bunch of records
        tini=datetime.now()
        dataset=[]
        claus=[]
        p=red.pipeline()

        # feed the pipeline with all keys we need
        for s in red.scan_iter(match=f'{self._name}:{match}'):
            p.hmget(s, self._fields)
            claus.append(s)

        # bring data in one operation
        a=0
        for h in p.execute():
            h[0]=claus[a]                        
            dataset.append(self._record(*h))
            a+=1
        logger.debug(f'{len(dataset)} records in {datetime.now()-tini}')
        return dataset        



class Record(Base):
    # not auto incremental, we provide the key

    def __init__(self, name, fields):                 
        super().__init__(name, fields)        
        # the key is the table name plus the key name
        self._format_key='{}:{}'

    def set(self, key, **kwargs):
        # set the record
        # check field names
        self.check_set(**kwargs)
        # format key
        key=self.fkey(key)
        # logger.debug(f'key is {key}')
        # set record        
        red.hmset(key, kwargs)
        return key

    def reset(self, key, **kwargs):
        # set the record with key key        
        return self.set(key, **kwargs)


class RecordAuto(Base):
    # autoincremental table

    def __init__(self, name, fields):
        super().__init__(name, fields)        
        # name of the counter
        self._counter_name=f'{self._name}counter'
        # the key is the table name plus the record number
        self._format_key='{}:{:012d}'
        # set the counter if not exists
        red.setnx(self._counter_name, 1)


    def get_key(self):
        # increment counter
        d=red.incr(self._counter_name)
        # return the key after incr in set
        return self.fkey(d)

    def set(self, **kwargs):
        # set the record
        self.check_set(**kwargs)
        key=self.get_key()        
        red.hmset(key, kwargs)
        return key

    def reset(self, key, **kwargs):
        # set the record with key key        
        red.hmset(self.fkey(key), kwargs)
        return key


class Area(RecordAuto):

    def __init__(self):
        super().__init__(
            'area', 
            ('description', 'channel', 'formatter')
            )
        

class Config(Record):
    def __init__(self):
         super().__init__(
            'config', 
            ('value',)
            )


config=Config()
config.set('user.name', value='pepito')
config.set('user.email', value='pepito@blahblah')
for a  in config.filter():
    print(a)

area=Area()
"""
k=area.set(description="Area número 1", channel={"otro channel": "lista"}, formatter="{}algo")
logger.debug(k)
k2=area.reset(11, description="Area cambiada")

logger.debug(
    area.get(1233)
    )
"""

logger.debug("--- all records -----")
area=Area()
k2=area.reset(21, description="Area cambiada")
for a in area.filter():
    print(a)
