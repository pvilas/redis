import redis
from datetime import datetime
from collections.abc import MutableMapping
import random
import string
import time
import binascii
from redisearch import Client, TextField, NumericField,\
                        TextField as DateField, TextField as DatetimeField,\
                        IndexDefinition, Query
import arrow
from loguru import logger
from wtforms import Form, BooleanField, StringField, HiddenField, validators
from werkzeug.datastructures import MultiDict
import tablib
from pagination import Pagination
import inspect
from collections import namedtuple

ver="0.5"

class rDocumentException(Exception):
    def __init__(self, message="Error message", doc:dict={})->None:
        logger.warning(f"rDocumentException ({type(self).__name__}) {doc.get('id', 'no_id')}: {message}. The document was {doc}.")
        super().__init__(message)

class rValidationException(rDocumentException):
    pass

class rSaveException(rDocumentException):
    pass

class rFKNotExists(rDocumentException):
    pass

class rDeleteException(rDocumentException):
    pass

class rDeleteFKException(rDocumentException):
    pass

class rBeforeSaveException(rDocumentException):
    pass

class rAfterSaveException(rDocumentException):
    pass

class rBeforeDeleteException(rDocumentException):
    pass

class rAfterDeleteException(rDocumentException):
    pass

class rSearchException(rDocumentException):
    pass

class BaseDefDoc(Form):
    """ The minimun definition template for a document """    
    id = StringField( 'Id', 
                       validators=[validators.Length(min=2, max=50), validators.InputRequired()],
                       render_kw=dict(indexed=True, on_table=False)
    ) 

class rBaseDocument(object):
    is_redis:bool=True
    query:str="*" # the default search string for this document

    class DefDoc(BaseDefDoc):
        # definition template for this document
        pass

    def __init__(self, db, prefix:str=None):
        """
            # rBaseDocument
            A RediSearch document but without imput validation

            ## Param
            conn - Redis connection
            prefix - name of the document i.e. PERSONA or None, in this case we take the name of the class

            ## Remarks
            After the index creation (first time) the index definition is no longer synced with 
            the database. You must maintain manually the changes on Redis or simply delete the
            index with: 
            
            ```> FT.DROPINDEX idx:movie```

            And let redis to recreate it. This is usually fast but can't be an option in a production environment.
        """
        self.db=db
        if not prefix:
            prefix=type(self).__name__.upper()
        self.prefix=prefix.upper()
        self.idx=Client(f"idx{self.db.delim}{self.prefix}", conn=db.r)

        # discover the first level of foreign keys and include it into the results
        self.discover=True 

        # build index list for RediSearch and columns for an html table of the data
        index=[]
        self.columns=[] # list to columns to appear in an auto generated html table
        self.dependant=[] # fields that depends of a foreign key
        self.index=[] # list of index field names        
        logger.debug(f"Members of document type {self.prefix}")
        for field in self.DefDoc():
            logger.debug(f"{field.name}({field.type}): {field.render_kw}")            
            if field.render_kw:
                # include field in index
                if field.render_kw.get('indexed', False):
                    self.index.append(field.name) # append to index field names list
                    if field.type in ('DecimalField', 'FloatField', 'IntegerField'):
                        index.append(NumericField(field.name, sortable=True))                    
                    else:
                        index.append(TextField(field.name, sortable=True))                     
                # include field in html table columns
                if field.render_kw.get('on_table', False):
                    self.columns.append(field.name)

        # build index 
        try:
            self.idx.create_index(
                index,
                definition=IndexDefinition(prefix=[f'{self.prefix}{self.db.delim}']))
        except Exception as ex:
            pass

    def info(self)->str:
        print(f"{self.prefix} information\n"+'='*30)
        print(f"Document members: {[(f.name,f.type) for f in self.DefDoc()]}")
        print(f"Indices: {self.index}")
        print(f"Foreign keys: {self.dependant}")
        l=[]
        for a, b in self.db.dependants:
            if b.prefix==self.prefix:
                l.append(a.prefix)
        print(f"Documents that depend of this document: {l}")
        print("")

    def k(self, id:str)->str:
        """ return a complete id: name+delim+id """
        return self.sanitize(id)

    def dict_to_namedtuple(self, p:dict, pref:str=None)->namedtuple:
        named=namedtuple(pref or self.prefix, p.keys())
        return named(**p)         

    def get(self, id:str)->namedtuple:
        """ return a document or None 
            ## Param
            * id - is the full id 
        """
        p=self.db.r.hgetall(self.sanitize(id))
        if p:
            return self.dict_to_namedtuple(self.db.r.hgetall(self.sanitize(id)))
        else:
            return None

    def validate_foreigns(self, doc:dict)->None:
        """ Called before save.
            Check if the object has the mandatory foreign fields and their values exists on the referenced document.

            ## Param 
            * doc - the dict to be saved in the document

            ## Exceptions
            rFKNotExists
        """
        for d, f in self.db.dependants:            
            if d.prefix==self.prefix:
                if doc.get(f.prefix.lower()) is None:
                    raise rFKNotExists(f"The member {f.prefix.lower()} of {self.prefix} does not exist in the document.", doc)
                if not self.db.r.exists(doc.get(f.prefix.lower())):
                    raise rFKNotExists(f"The member {d.prefix}.{f.prefix.lower()}, with value {doc.get(f.prefix.lower())}, does not exist as a foreign key of {f.prefix.upper()}", doc)

    def before_save(self, doc:dict)->dict:
        """ Check, sanitize, etc... 
            Raise Exception on error
            ## Param            
            * doc - The dict to be saved, before perform the checkin

            ## Exceptions
            rBeforeSaveException
            e.g. if doc.get('field_name') is None:
                    raise rBeforeSaveException(f"field_name can not be None")

            ## Return            
            The checked, sanitized doc
        """                
        self.validate_foreigns(doc)
        return doc        

    def sanitize(self, id:str)->str:
        """ Sanitize and id before use it 
            
            ## Param 
            * id - the str to sanitize
            ## Exceptions
            rsaveException if the key is invalid (len==0)
        """
        # sanitize the id -> remove non alpha-numeric characters and the delimitator from the id
        id=self.db.delim.join([self.db.key_sanitize(t) for t in id.split(self.db.delim)])

        # remove any delim character after the document name
        if id.startswith(self.prefix+self.db.delim):
            id_part=''.join([t for t in id.split(self.db.delim)[1:]])
            if len(id_part)==0:
                raise rSaveException("Len of id cant be zero", {'id':id})
            id=f"{self.prefix}{self.db.delim}{id_part}"
        else:
            # prefix the id with the document name
            id = self.db.k(self.prefix, id)
        return id.upper()

    def after_save(self, doc:dict, id: str) -> None:
        """ Do tasks after save
            ## Param 
            * doc - the saved dict
            * id  - the id of the saved doc
            ## Exceptions
            rAfterSaveException
        """
        return None

    def save(self, **doc:dict)->str:        
        try:
            # if there isn't an id field, create and populate it
            if doc.get('id', None) is None: 
                # the counters always ends with _KEY
                NOM_COMPTADOR=f"{self.prefix.upper()}_KEY"        
                # create the counter if it not exists
                n=self.db.r.get(NOM_COMPTADOR)                
                if n is None:
                    self.db.r.set(NOM_COMPTADOR, 1)
                    n=1      
                # rpad with zeros
                doc['id']=f'{n}'.rjust(8, '0')
                self.db.r.incr(NOM_COMPTADOR)
            
            # sanitize the id
            doc['id']=self.sanitize(doc['id'])

            # call before_save, can raise an exception
            doc=self.before_save(doc)        

            # si no hi ha camp de creacio, el cream i el populam
            if doc.get('created_at', None) is None:
                doc['created_at']=self.db.now()
            
            # el camp updated_on el populam sempre
            doc['updated_at']=self.db.now()

            # salvam el diccionari
            self.idx.redis.hset(doc['id'], mapping=doc)

            # cridam after save
            self.after_save(doc, doc['id'])

            return doc['id']
        except Exception as ex:
            logger.error(f"Database error while saving doc id {doc.get('id')}: {ex}")
            raise rSaveException(ex, doc)

    def before_delete(self, id:str)->None:
        """ Check if we can delete this document 
            At this stage, we can delete if this document is not the key of a foreign key
            raising an Exception if not
            ## Param
            * id - is the complete id prefix:id
            ## Exception
            rBeforeDeleteException
        """        
        id=self.sanitize(id) 
        for d in self.db.dependants:
            # dependants està organitzat com p.e. (PERSONA, PAIS)
            # miram si la dependència s'aplica a aquest document
            if (self.prefix==d[1].prefix): # volem esborrar un pais i persona en depén
                # print(f"{d[0].prefix} depén de {self.prefix}, comprovant si hi ha algun doc a {d[0].prefix} amb la clau {id}")
                cad=f'@{d[1].prefix.lower()}:{id}'
                # print(f"La cadena de busqueda a {d[0].prefix} es {cad}")                
                if d[0].search(cad).total>0:
                    raise rDeleteFKException(f"Cant delete {id} of {self.prefix} because there are document of {d[0].prefix} that have this key.", {"id":id})

    def after_delete(self, id:str)->None:
        """ Perform some action after deletion
            ## Param
            * id - the complete id prefix:id
            * doc - the deleted document
            ## rAfterDeleteException
        """
        pass

    def delete(self, id:str)->None:
        """ Remove a key from the hash.
            before_delete can throw an Exception

            ## Param
            * id - the complete id prefix:id            

            ## Exceptions
            rDeleteException
        """
        id=self.sanitize(id)
        self.before_delete(id)
        try:
            self.db.r.delete(id)
        except Exception as ex:
            raise rDeleteException(ex, {'id':id})
        self.after_delete(id)        

    def search(self, query:str, start:int=0, num:int=10, sort_by:str='id', direction:bool=True, slop=0)->list:
        """ perform a query with the index
            ## Param
            * query - is the string query
            * start - page form record start
            * num - number of records to include into the result
            * sort_by - field to order by, defaul: *id*
            * direction - asc True desc False
            * slop - number of non matched terms (Levensthein distance), default: *0*
            ## Exception
            rSearchException
            ## Return 
            A list of records
        """
        try:            
            q=Query(query).slop(slop).sort_by(sort_by, direction).paging(start, num)
            result=self.idx.search(q)
            if not self.discover or len(self.dependant)==0:
                return result
            # discover first level foreign keys
            docs=result.docs
            if result.total>0 and len(self.dependant)>0:
                docs_with_discover=[] # new list of docs
                # for each document
                for doc in self.db.docs_to_dict(result.docs):
                    n={}
                    # for each member of the doc
                    for k, v in doc.items():                        
                        # if this field is dependant
                        if k.upper() in self.dependant: 
                            # include a get of the foreign key as member_name.data
                            n[k]=self.dict_to_namedtuple(self.db.r.hgetall(v), k.upper())
                        else:
                            n[k]=v                      
                    # append to the list of new docs   
                    docs_with_discover.append(self.dict_to_namedtuple(n))
                docs=docs_with_discover
            # return the result as a resisearch result
            r=namedtuple('documents', ['total', 'docs'])
            return r(total=result.total, docs=docs)
        except Exception as ex:
            raise rSearchException(str(ex), {'query':query})
    
    def paginate(self, query:str, page:int=1, num:int=10, sort_by:str='id', direction:bool=True, slop:int=0)->Pagination:
        try:     
            tic = time.perf_counter()       
            start=(page-1)*num
            # count total of docs to calculate the total of pages
            total=self.idx.search(Query(query).slop(slop).paging(0, 0)).total
            # construct the query, paginated start and num
            q=Query(query).slop(slop).sort_by(sort_by, direction).paging(start, num)
            # perform the query
            items=self.idx.search(q).docs
            elapsed_time = time.perf_counter() - tic
            logger.debug(f"Pagination over {self.prefix}({query}) with {num} of {total} results done in {(elapsed_time*1000):0.3f}ms")
            p=Pagination(page=page, per_page=num, total=total, items=items)
            return p
        except Exception as ex:
            raise rSearchException(str(ex), {'query':query})


class rWTFDocument(rBaseDocument):
    
    class DefDoc(BaseDefDoc):
        pass
         
    class AddForm(DefDoc):
        pass

    class EditForm(AddForm):
        id = HiddenField()        

    class DeleteForm(EditForm):
        id = HiddenField()        

    class SearchForm(Form):
        pass

    def __init__(self, db, prefix:str=None):
        """
            # rWTFDocument
            És un document de RediSearch amb validació de l'imput usant WTForms

            AddForm, EditForm, DeleteForm, SearchForm són les fitxes per defecte per fer
            les operacions. 

            AddForm s'utilitza com a base per a les **validacions** de les dades.

            ## Param
            conn - sa connexió amb redis
            prefix - el nom de sa taula, passarà a majúscules, sense els dos punts
        """
        super().__init__(db, prefix)
                
    def validate(self, doc:dict, use_form:Form=None)->dict:
        """ validate the input using an WTForm """
        if not doc:
            raise rBeforeSaveException('Cant validate a null document')

        # create the addform with the doc
        try:
            form_obj=use_form or self.DefDoc
            form=form_obj(doc)            
            if form.validate():
                doc=form.data
                return doc
            else:
                raise rValidationException(f'There are validation errors: {form.errors}', doc)
        except Exception as ex:
            raise rBeforeSaveException(f"Error validating: {ex}", doc)


    def before_save(self, doc:dict)->dict:
        """ 
            Check imput by validating an wtform
        
            Check, sanitize, etc... 
            Raise Exception on error
            ## Param            
            * doc - The dict to be saved, before perform the checkin

            ## Exceptions
            rBeforeSaveException
            e.g. if doc.get('field_name') is None:
                    raise rBeforeSaveException(f"field_name can not be None")

            ## Return            
            The checked, sanitized doc
        """                
        doc=super().before_save(doc)
        return self.validate(MultiDict(doc))

class rBasicDocument(rWTFDocument):
    class DefDoc(BaseDefDoc):
        description = StringField( 'Descripción', 
                                   validators = [validators.Length(max=50), validators.InputRequired()],
                                   render_kw=dict(indexed=True, on_table=True)) 

    class DeleteForm(DefDoc):
        id = HiddenField()        
        description = StringField('Descripción', render_kw={'readonly':True}) 

    def __init__(self, db, prefix:str=None):
        """ a document with id and description """
        super().__init__(db, prefix)

class rDatabase(object):

    def __init__(self, r):
        """ a redis database with a collection of descriptions """
        self.r = r
        self.dependants=[]
        self.delim='.' # do not use {:, /, #, ?} or anything related with url encoding

    def set_fk(self, definition:rWTFDocument, depends_of: rWTFDocument)->None:
        self.dependants.append((definition, depends_of))
        definition.dependant.append(type(depends_of).__name__.upper())        

    def k(self, *id:str)->str:
        """ return a complete id: name+delim+id """
        return self.delim.join([self.key_sanitize(s) for s in id])

    def key_sanitize(self, s:str)->str:
        """ sanitizes a key of an id: only letters or digits and uppercase """
        l=[]
        for i in s:
            if i in set(string.ascii_letters + string.digits):
                l.append(i)
        return ''.join(l).upper()

    def now(self)->str:
        return str(arrow.utcnow().to('local'))

    def today(self)->str:
        return str(arrow.utcnow().format('YYY-MM-DD'))

    def id_generator(self, size:int=24, chars:set=string.ascii_uppercase + string.digits)->str:    
        """ return an uuid string """
        random.seed(444)
        return ''.join(random.choice(chars) for _ in range(size))

    def docs_to_dict(self, docs:list)->list:
        """ transform docs in a list of dicts """
        reslist = []
        for doc in docs:            
            fields = {}
            for field in dir(doc):
                if (field.startswith('__')):
                    continue
                fields.update({ field : getattr(doc, field) })            
            reslist.append(fields)
        return reslist

    def tabbed(self, docs:list)->str:
        # return a tablib with all data    
        # print(db.tabbed(persona.search("*", sort_by="apellidos").docs))    
        if len(docs)>0:                        
            docs=self.docs_to_dict(docs)            
            keys=[k for k in docs[0].keys()]            
            tab=tablib.Dataset(headers=keys)        
            for p in docs:
                tab.append(p.values())
            return tab
        else:
            return ''