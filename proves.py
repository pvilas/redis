import redis
from loguru import logger
from datetime import datetime
from collections import namedtuple
import tablib

red=redis.Redis(
    host='localhost',
    decode_responses=True
)
 
class Base:

    def __init__(self, 
                 name='table',
                 fields=(),
                 indexes=()): 
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
        self._format_id_index=f'{self._name}.id.index'
        # to order fields
        self._monotonic=f'{self._name}.monotonic'


    def fkey(self, k):
        # return name:k
        return self._format_key.format(self._name, k)

    def check_set(self, **kwargs):
        # checks if each field exists on field list
        for k in kwargs.keys():
            if not k in self._fields:
                raise Exception(f'field {k} is not on the {self._name} fieldlist: {self._fields}')

    def all_none(self, arr):
        # check if all elements of the list are None
        for t in arr:
            if t is not None:
                return False
        return True

    def make_record(self, record):
        """ makes and returns a namedtuple with the record 
            it should be overriden if additional logic or formatting was need
            :record: the record given by redis
            :return: the named tuple
        """
        return self._record(*record)


    def get(self, key):
        """ get the record
            :key: the key to find out
            :return: a namedtuple with record or None
        """
        #logger.debug(f'key is {key} and fkey is {self.fkey(key)}')
        rec=red.hmget(self.fkey(key), self._fields)
        # check if all fields are None
        # logger.debug(f'rec is {rec}')
        if self.all_none(rec):
            # logger.debug(f"{key} is None")
            return None
        rec[0]=self.fkey(key)
        #logger.debug(f'record is {dir(self._record)}')        
        return self.make_record(rec)

    def filter(self, match='*'):
        # return a bunch of records
        tini=datetime.now()
        dataset=[]
        claus=[]
        p=red.pipeline()

        # feed the pipeline with all keys we need
        for s in red.scan_iter(match=f'{self._name}:{match}'):
            # for s in red.zrangebyscore(self._format_id_index, '-inf', '+inf'):
            p.hmget(s, self._fields)
            claus.append(s)

        # bring data in one operation
        a=0
        for h in p.execute():
            h[0]=claus[a]                        
            dataset.append(self.make_record(h))
            a+=1
        logger.debug(f'{len(dataset)} records in {datetime.now()-tini}')
        return dataset        

    def get_tab(self):
        # return a tablib with all data
        tab=tablib.Dataset(headers=self._fields)
        for t in self.filter():            
            tab.append([f for f in t])
        return tab

    def _new(self, key, kwargs):
        """ insert or reset a record (call only from new) 
        """
        red.hmset(key, kwargs)
        # make indexes...
        # always id index
        d=red.incr(self._monotonic)
        red.zadd(self._format_id_index, d, key)
        # make other indexes
        # for i in self._indexes:


    def __str__(self):
        # print all records
        return f"----- {self._name} ------\n{self.get_tab()}"        

class Record(Base):
    # not auto incremental, we provide the key

    def __init__(self, name, fields, indexes=()):                 
        super().__init__(name, fields, indexes)        
        # the key is the table name plus the key name
        self._format_key='{}:{}'

    def new(self, key, **kwargs):
        """ set the record
            :key: the unique key of the record
            :kwargs: dict of fields and values (the field names are checked)
            :return: the get operation of the new record
        """
        # check field names
        self.check_set(**kwargs)
        # format key
        key=self.fkey(key)
        # logger.debug(f'key is {key}')
        # set record        
        # red.hmset(key, kwargs)
        try:
            self._new(key, kwargs)
            return self.get(key)
        except Exception as ex:
            msg=f'Inserting record on {self._name}, key {key}: {ex}'
            logger.error(msg)
            raise Exception(msg)

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


    def make_key(self):
        # increment counter
        d=red.incr(self._counter_name)
        # return the key after incr in set
        return (self.fkey(d), d)

    def new(self, **kwargs):
        # set the record
        self.check_set(**kwargs)
        key, d =self.make_key()     
        try:
            self._new(key, kwargs)
            return self.get(d)
        except Exception as ex:
            msg=f'Inserting record on {self._name}, key {key}: {ex}'
            logger.error(msg)
            raise Exception(msg)

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



red.zadd('algo', {"Pere", 1), ("Juanito", 2),) )

print(red.zrangebyscore('algo', min=0, max=10))

# redis.execute_command('ZADD', set_name, 'NX', score, key)

exit(0)
config=Config()
config.new('user.name', value='pepito')
config.new('user.email', value='pepito@blahblah')

print(config)

area=Area()

k=area.new(description="Area número 1", channel="otro channel", formatter="{}algo")
logger.debug(k.description)
k2=area.reset(11, description="Area cambiada")

logger.debug(
    area.get(1233)
    )

print(area)