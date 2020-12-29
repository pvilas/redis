import redis
from datetime import datetime
from collections.abc import MutableMapping
import random
import string
from time import time
import binascii
from redisearch import Client, TextField, IndexDefinition, Query
import arrow

DELIM=':'


def now()->str:
    return str(arrow.utcnow().to('local'))

def today()->str:
    return str(arrow.utcnow().format('YYY-MM-DD'))


r=redis.Redis(
    host='localhost',
    decode_responses=True # decode all to utf-8
)

"""
import datetime
today = str(datetime.date.today())
print(f"today is {today}")

visitors = {"dan", "jon", "alex"}
r.sadd(today, *visitors)
print(f"Today {today} members ({r.scard(today)}) are {r.smembers(today)}")
"""

def count_elapsed_time(f):
    def wrapper():
        # Start counting.
        start_time = time()
        # Take the original function's return value.
        ret = f()
        # Calculate the elapsed time.
        elapsed_time = time() - start_time
        print("Elapsed time: %0.10f seconds." % elapsed_time)
        return ret
    
    return wrapper

def id_generator(size=24, chars=string.ascii_uppercase + string.digits):    
    return ''.join(random.choice(chars) for _ in range(size))

def str_to_int(val:str)->int:
    return int(binascii.hexlify(val.encode('utf-8')), 16)

def key_sanitize(s:str)->str:
    l=[]
    for i in s:
        if i in set(string.ascii_letters + string.digits):
            l.append(i)
    return ''.join(l).upper()

def save(r: redis.Redis, prefix:str, obj:dict)->str:            
    # si no hi ha camp d'index, el cream i el populam
    if obj.get('id', None) is None: 
        # el nom de sa clau acaba en _KEY
        NOM_COMPTADOR=f"{prefix.upper()}_KEY"        
        # miram si està el comptador de ids
        n=r.get(NOM_COMPTADOR)
        # print(f"compatdor es {NOM_COMPTADOR} = {n}")
        if n is None:
            r.set(NOM_COMPTADOR, 1)
            n=1      
        # print(f"n = {n}")              
        obj['id']=f'{n}'.rjust(8, '0') #f'{n:08}'
        r.incr(NOM_COMPTADOR)
    else: # si hi ha camp d'index, el sanitizam
        obj['id']=key_sanitize(obj['id'])
    
    # si no hi ha camp de creacio, el cream i el populam
    if obj.get('created_on', None) is None:
        obj['created_on']=now()
    
    # el camp updated_on el populam sempre
    obj['updated_on']=now()

    # cream la clau
    NOM_CLAU = f"{prefix.upper()}:{obj['id']}"
    # print(f"La clau es {NOM_CLAU}")

    # salvam el diccionari
    r.hset(NOM_CLAU, mapping=obj)

    return NOM_CLAU

@count_elapsed_time
def haz(r):
    start_time = time()
    for a in range(0, 10):
        persona=dict(dni=f'41{a}', nombre=f'Pepe {a} illo', apellidos=id_generator())  
        save(r, 'PERSONA', persona)
    elapsed_time = time() - start_time
    print("Creation time: %0.10f seconds." % elapsed_time)



random.seed(444)

"""
Instalar redis

brew install redis


Per compilar redisearch

https://docs.deistercloud.com/content/Databases.30/Redis/index.xml?embedded=true&navbar=0&param-iframe=index-iframe#589232e85dfa26f2270fee313efa40b5


brew install cmake
cd $HOME/src
git clone --recursive https://github.com/RediSearch/RediSearch.git 
cd RediSearch
make

### Copiar el mòdul complilat a /usr/local/etc

```cp build/redisearch.so /usr/local/etc```

I afeixir es loadmodule build/redisearch.so a /usr/local/etc/redis.conf


```echo "loadmodule /usr/local/etc/redisearch.so" >> /usr/local/etc/redis.conf```

Arrancar redis 

```brew services start redis```

Per veure si s'ha carregat es mòdul search

redis-cli module list




To have launchd start redis now and restart at login:
  brew services start redis
Or, if you don't want/need a background service you can just run:
  redis-server /usr/local/etc/redis.conf
==> Summary



Possible conflicting files are:
/usr/local/bin/redis-cli
/usr/local/bin/redis-server
==> Caveats
To have launchd start redis now and restart at login:
  brew services start redis
Or, if you don't want/need a background service you can just run:
  redis-server /usr/local/etc/redis.conf


Tutorial
https://github.com/RediSearch/redisearch-getting-started


"""
# ZRANGEBYLEX PERSONA_NOMBRE_INDEX "[PEPE" "[PEPE\xff" limit 98765 99001


idx_persona=Client("idx:persona", conn=r)


# esborram base de dades
r.flushdb()
"""
# cream index damunt persona
client.create_index(
    (TextField('id', sortable=True), 
     TextField('dni', sortable=True), 
     TextField('nombre', sortable=True), 
     TextField('apellidos', sortable=True)), 
    definition=IndexDefinition(prefix=['PERSONA:']))
"""

start_time = time()
for a in range(0, 100000):
    persona=dict(dni=f'41{a}', nombre=f'Pepe {a} illo', apellidos=id_generator())  
    save(r, 'PERSONA', persona)
elapsed_time = time() - start_time
print("Creation time: %0.10f seconds." % elapsed_time)


print(f"Number of keys is {r.dbsize()}")

exit(0)

start_time = time()
# print(get(r, 'PERSONA', 'id1095'))
g=[]
# filtrar tots els que el seu nom comenci per Pepe44
for k in r.sort(f'PERSONA_NOMBRE_INDEX',  alpha=True, desc=False):        
    t=r.hgetall(f"PERSONA:{k}")
    if t['nombre'].startswith("Pepe44"):
        g.append(t)
elapsed_time = time() - start_time
print("search time: %0.10f seconds." % elapsed_time)
print(len(g))
print(g)


"""
l=get_all(r, 'PERSONA', start=3456, num=100)
print(len(l))
print(l)
"""

exit(0)







# print(get_record_id(r, 'PERSONA', 'id99'))
g=get_table(r, 'PERSONA', fields=('nombre',), start=100)
for a in range(10):
    print(g[a])
print(len(g))



exit(0)

from loguru import logger
from collections import namedtuple
import tablib
from random import choice
from string import ascii_uppercase


class Base:
    """
    Handles a Redis *table*. 
    ------------------------
    This class tries to mimic a table to work with redis

    It has the following characteristics:    
    - Easy field access by dot syntax (using a named tuple).
    - The key is always name *id*.
    - Autoformats the key.
    - Maintains a direct and a reverse index for each indexed field.
    - Output table format by [tablib](https://github.com/jazzband/tablib). We can view/save in various formats.
    - Find out a record by its key with *get*
    - Filter a table by its fields with *filter*
    """
    def __init__(self, 
                 name='table',
                 fields=(),
                 indexes=()): 
        """ Base for records
            -----------------
            :name: the name of the 'table'
            :fields: tuple with field names used in get
            :indexes: tuple with fields that are indexes
        """                
        super().__init__()    
        self._name=name
        # create named tuple from field names
        self._fields=('id',)+fields
        self._record=namedtuple(name, self._fields)
        # default key format, note that autoincrmental has its own
        self._format_key='{}:{}'
        self._format_id_index=f'{self._name}.id.index'
        self._format_id_index_reverse=f'{self._name}.id.reverse.index'
        # to order fields
        self._monotonic=f'{self._name}.monotonic'
        self._indexes=indexes

    def fkey(self, k):
        """ 
        Formats the key for this record.
        --------------------------------
        The key is always formad by the name of the *table* plus the key value 
        The standar separator is the **:** character

        Parameters:
            :k: the key of the record

        Returns:
            - table_name:key
        """
        # return name:k
        # logger.debug(f'formating key {k}')
        k=str(k)
        k=k.replace(':', '') # remove the separator from key
        return f'{self._name}:{k}'
        # return self._format_key.format(self._name, k)

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
        """ Makes and returns a namedtuple with the record 
            ----------------------------------------------
            it should be overriden if additional logic or formatting was need
            :record: the record given by redis
            :return: the named tuple
        """
        return self._record(*record)

    def record_as_dict(self, r):
        """ return an ordereddict as a dict """
        d={}
        for t in r:
            print(t)
            d[t[0]]=t[1]
        return d

    def get_cardinal(self, key):
        # return the cardinality if exist
        t=key.split(':')
        if len(t)>0:
            return t[1]
        else:
            return '-1' 

    def get(self, key):
        """ Get the record with key *key*
            -----------------------------
            Parameters:            
            - :key: the key to find out
            Return:
            - :return: a namedtuple with record or None if the record does not exist
        """
        tini=datetime.now()
        #logger.debug(f'key is {key} and fkey is {self.fkey(key)}')
        rec=red.hmget(self.fkey(key), self._fields)
        # check if all fields are None
        # logger.debug(f'rec is {rec}')
        if self.all_none(rec):
            # logger.debug(f"{key} is None")
            return None
        # take the key part only, not the table name
        rec[0]=self.fkey(key).split(':')[1]
        logger.debug(f'Record retrieved in {datetime.now()-tini}')
        #logger.debug(f'record is {dir(self._record)}')        
        return self.make_record(rec)

    def filter(self, match='*', order='id'):
        """"
        Filters a dataset
        -----------------
        Returns all *records* that match *match* in order *order*. 
        It uses a redis [pipeline](https://redis.io/topics/pipelining).

        Parameters:
        - :match: the matching string
        - :order: the field to order the results
        Return:
        - :dataset: The filtered result ordered by *order* or None
        """
        # return a bunch of records
        tini=datetime.now()
        dataset=[]
        claus=[]
        p=red.pipeline()

        # feed the pipeline with all keys we need
        #for s in red.scan_iter(match=f'{self._name}:{match}'):
        #    p.hmget(s, self._fields)
        #    claus.append(s)
        # logger.debug(f"using index {order}")
        r=red.zscan(f'{self._name}.{order}.index')[1]
        # logger.debug(f"index is {r}")
        for t in r:
            # for s in red.zrangebyscore(self._format_id_index, '-inf', '+inf'):
            # take the key part
            g=t[0].split(':')[1]            
            # logger.debug(f"g is {g}")
            p.hmget(self.fkey(g), self._fields)
            claus.append(g)

        # bring data in one operation
        a=0
        for h in p.execute():
            # logger.debug(f'Record is {h}')
            h[0]=claus[a]                        
            dataset.append(self.make_record(h))
            a+=1
        # log time recovery statistics
        logger.debug(f'{len(dataset)} records in {datetime.now()-tini}')
        return dataset        

    def get_tab(self, match='*', order='id'):
        # return a tablib with all data
        logger.debug(f"order is {order}")
        tab=tablib.Dataset(headers=self._fields)
        for t in self.filter(match=match, order=order):            
            tab.append([f for f in t])
        return tab

    def _new(self, key, kwargs):
        """ 
            Insert or reset a record (call only from new) 
            ---------------------------------------------
            Maintains the indexes
        """
        red.hmset(key, kwargs)
        # make indexes...
        # always id index
        # d=red.incr(self._monotonic)
        # add to zset the key is id:key and the score comes from key_to_cardinal
        # if key_to_cardinal is 0 it is because the key is lexicographical
        c=self.get_cardinal(key)
        red.zadd(self._format_id_index, { f'{c}:{c}':0 })
        # make other indexes
        for i in self._indexes:
            iname=f'{self._name}.{i}.index'
            # if the field value of the index is present
            if i in kwargs.keys():
                red.zadd(iname, { f"{kwargs[i]}:{self.get_cardinal(key)}":0 })    
            # logger.debug(f'record is {r} and value is {v}')
            # assign the field value plus the key
            


    def rebuilt_index(self):
        """
            Rebuilds the indexes in the case of a change in fields
            ------------------------------------------------------
            Must be called each time we change the list of indexes
        """
        # delete indices
        for i in self._indexes:
            iname=f'{self._name}.{i}.index'
            red.delete(iname)
        # delete id index
        logger.info(f"deleting id index of {self._name}")
        red.delete(self._format_id_index)
        # build indices
        logger.info("Rebuilding id index")
        for s in red.scan_iter(match=f'{self._name}:*'):
            # add key to the id index
            red.zadd(self._format_id_index, {s:0})
            r=red.hmget(s, self._fields)
            # for each index
            for i in self._indexes: # index is the name of the field                
                iname=f'{self._name}.{i}.index'
                #logger.debug(f"{iname}")
                #logger.debug(f"index is {i} and position is {self._fields.index(i)}")

                v=r[self._fields.index(i)]
                # logger.debug(f'record is {r} and value is {v}')
                # assign the field value plus the key
                red.zadd(iname, { f"{v}:{self.get_cardinal(s)}":0 })

    def __str__(self):
        # print all records in tabular fashion
        return f"----- {self._name} ------\n{self.get_tab()}"        

class Table(Base):
    """ A table with non incremental key
        --------------------------------

        We provide the key with each record
    """

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
        return red.set(key, **kwargs)


class AutoTable(Base):
    """
        An autoincremental table
        ------------------------
        The key is incremented monotonically
    """

    def __init__(self, name, fields, indexes=()):
        super().__init__(name, fields, indexes)        
        # name of the counter
        self._counter_name=f'{self._name}counter'
        # the key is the table name plus the record number
        self._format_key='{:012d}'
        # set the counter if not exists
        red.setnx(self._counter_name, -1)


    def make_key(self):
        # increment counter
        d=red.incr(self._counter_name)
        # return the key after incr in set
        return (self.fkey(self._format_key.format(d)), d)

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


class Area(AutoTable):
    """
        A test table Area with autoincremental key
        ------------------------------------------
        It has three fields, two indexes

    """
    def __init__(self):
        super().__init__(
            'area', 
            ('description', 'channel', 'formatter'),
            ('description', 'formatter',)
            )
        

class Config(Table):
    """
        A test table without autoincremental key
        ---------------------------------------- 
    """
    def __init__(self):
         super().__init__(
            'config', 
            ('value',)
            )



area=Area()

config=Config() # create table Config
config.new('user.name', value='pepito')   # append or update record with key `user.name`
config.new('user.email', value='pepito@blahblah') # append or update record with key `user.email`

#for r in config.filter():
#    print(r.id, r.value) 
#print(config.get_tab(match='*'))

# get record by key
#print(config.get('user.name'))    

print(config.filter(match='user2.*'))

exit(0)

area=Area() # create table Area

# create some records 
for a in range(0, 9):
    r=''.join(choice(ascii_uppercase) for i in range(12))
    g=''.join(choice(ascii_uppercase) for i in range(12))
    k=area.new( description=f"Area número {r}", 
                channel=f"channel {a}", 
                formatter=g
    )

# area.rebuilt_index()
print('ordre natural')
print(area)

print('ordre formatter')
print(area.get_tab(order='formatter'))

print('ordre description')
print(area.get_tab(order='description'))


"""
red.zadd('algo', {"XXJesús":1,  "Antonio": 3, "Pere":1, "Juanito":2} )

print(red.zrangebyscore('algo', min=0, max=10))

# redis.execute_command('ZADD', set_name, 'NX', score, key)

exit(0)
"""

"""
r=red.zscan('area.id.index')[1]
for t in r:
    print(t[0])
    #red.delete(r)

exit(0)
"""


"""    
logger.debug(k.description)
k2=area.reset(11, description="Area cambiada")

logger.debug(
    area.get(1233)
    )
"""

"""
logger.debug('Antes rebuild')
print(area)

area.rebuilt_index()
logger.debug('despues rebuilt')
print(area)
"""