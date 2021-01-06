import redis
from datetime import datetime
import random
import string
import time
from redisearch import Client, TextField, NumericField,\
                        TextField as DateField, TextField as DatetimeField,\
                        IndexDefinition, Query
import arrow
from loguru import logger
from wtforms import Form, BooleanField, StringField, HiddenField, validators
from werkzeug.datastructures import MultiDict
from pagination import Pagination
from dotmap import DotMap

ver="0.5"

class rDocumentException(Exception):
    def __init__(self, message="Error message", doc:dict={})->None:
        logger.warning(f"rDocumentException ({type(self).__name__}) {doc.get('id', 'no_id')}: {message}. The document was {doc}.")
        super().__init__(message)

class rValidationException(rDocumentException):
    pass

class rTypeException(rDocumentException):
    pass

class rUniqueException(rDocumentException):
    pass

class rSaveException(rDocumentException):
    pass

class rFKNotExistsException(rDocumentException):
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

class BaseDefinition(Form):
    """ The minimun definition template for a document """    
    id = StringField( 'Id', 
                       validators=[validators.Length(min=2, max=50), validators.InputRequired()],
                       render_kw=dict(indexed=True, on_table=False)
    ) 


class BaseDocument(object):
    is_redis:bool=True
    query:str="*" # the default search string for this document

    class Definition(BaseDefinition):
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

        # build index list for RediSearch and columns for an html table of the data
        index=[]
        self.columns=[] # list to columns to appear in an auto generated html table
        self.dependant=[] # fields that depends of a foreign key
        self.index=[] # list of index field names        
        self.uniques=[] # list of fields that must be uniques
        logger.debug(f"Members of document type {self.prefix}")
        for field in self.Definition():
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
                # the field has unique values
                if field.render_kw.get('unique', False):
                    self.uniques.append(field.name) # append to uniques
                    if not field.name in self.index: # append to index list
                        self.index.append(field.name)
                        if field.type in ('DecimalField', 'FloatField', 'IntegerField'):
                            index.append(NumericField(field.name, sortable=True))                    
                        else:
                            index.append(TextField(field.name, sortable=True))                     

        # build index 
        try:
            self.idx.create_index(
                index,
                definition=IndexDefinition(prefix=[f'{self.prefix}{self.db.delim}']))
        except Exception as ex:
            pass

    def info(self)->str:
        s=f"{self.prefix} information"
        print(f"\n{s}\n"+'='*len(s))
        print(f"Document members: {[(f.name,f.type) for f in self.Definition()]}")
        print(f"Indices: {self.index}")
        print(f"Foreign keys: {self.dependant}")
        l=[]
        for a, b in self.db.dependants:
            if b.prefix==self.prefix:
                l.append(a.prefix)
        print(f"Documents that depend of this document: {l}")
        print(f"Unique members: {self.uniques}")
        print(f"Number of documents: {self.search('*').total}")
        print("")

    def k(self, id:str)->str:
        """ return a complete id: name+delim+id """
        return self.sanitize(id)

    def get(self, id:str)->DotMap:
        """ return a document or None 
            ## Param
            * id - is the full id 
        """
        p=self.db.r.hgetall(self.sanitize(id))
        if p:
            return DotMap(self.unescape_doc(self.discover(p)))
        else:
            return None

    def validate_foreigns(self, doc:dict)->None:
        """ Called before save.
            Check if the object has the mandatory foreign fields and their values exists on the referenced document.

            Also check the uniqueness of unique fields

            ## Param 
            * doc - the dict to be saved in the document

            ## Exceptions
            rFKNotExists, rUnique
        """
        for d, f in self.db.dependants:            
            if d.prefix==self.prefix:
                if doc.get(f.prefix.lower()) is None:
                    raise rFKNotExistsException(f"The member {f.prefix.lower()} of {self.prefix} does not exist in the document.", doc)
                if not self.db.r.exists(doc.get(f.prefix.lower())):
                    raise rFKNotExistsException(f"The member {d.prefix}.{f.prefix.lower()}, with value {doc.get(f.prefix.lower())}, does not exist as a foreign key of {f.prefix.upper()}", doc)
        
        # test uniqueness
        for d in self.uniques:
            q=f"@{d}:\"{doc.get(d)}\""            
            if doc.get(d) and self.search(q).total>0:
                print(f"testing uniqueness of {d} by searching {q}")
                raise rUniqueException(f"Value {doc.get(d)} already exists in document {self.prefix}, member {d}")

    def escape_doc(self, doc:dict)->dict:
        """ qescape all str fields """
        esc_doc={}
        for k, v in doc.items():
            if type(v).__name__=='str':
                esc_doc[k]=self.db.qescape(v)
            else:
                esc_doc[k]=v
        return esc_doc

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
        # 1. check types and escape strings
        # check if all members of the doc are string, int or float
        new_doc={}
        try:        
            for k, v in doc.items():
                # print(f"type of {k} is {type(v).__name__}")
                # if it is a DotMap, only include the id or None
                t=type(v).__name__
                if t in('DotMap', 'dict'):
                    new_doc[k]=v.get('id', None)
                elif t in ('int', 'NoneType') :
                    new_doc[k]=v
                elif t in ('str',):
                    new_doc[k]=self.db.qescape(v)
                elif t in ('Arrow', 'datetime', 'date', 'time'):
                    new_doc[k]=str(arrow.get(v)) # normalize to iso
                else:
                    new_doc[k]=str(v)
        except Exception as ex:
            raise rTypeException(f"Error checkin datatypes, only str, int or float allowed: {ex}")
        
        # 2. validate fks
        self.validate_foreigns(new_doc)
        return new_doc

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

    def s(self, **doc:dict)->str:
        """ call save with func params as a dict """
        return self.save(doc)

    def save(self, doc:DotMap)->str:                
        """ save the dictionary and return his id """
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

    def unescape_doc(self, doc:dict)->dict:
        """ qunescape all str fields """
        esc_doc={}
        for k, v in doc.items():
            if type(v).__name__=='str':
                esc_doc[k]=self.db.qunescape(v)
            else:
                esc_doc[k]=v
        return esc_doc

    def discover(self, doc: dict)->DotMap:
        """ discover first level foreign keys and include the result into the dict """        
        n={}
        # for each member of the doc
        for k, v in doc.items():                        
            # if this field is dependant
            if k.upper() in self.dependant: 
                # include a get of the foreign key as member_name.data                
                n[k]=self.unescape_doc(DotMap(self.db.r.hgetall(v)))
            else:
                if type(v).__name__=='str':
                    n[k]=self.db.qunescape(v)
                else:
                    n[k]=v
        return DotMap(n)

    def search(self, query:str="*", start:int=0, num:int=10, sort_by:str='id', direction:bool=True, slop=0)->list:
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
            if len(self.dependant)==0:
                return result
            # discover first level foreign keys
            docs=result.docs
            if result.total>0: # and len(self.dependant)>0:
                docs_with_discover=[] # new list of docs
                # for each document                
                for doc in self.db.docs_to_dict(result.docs):
                    # append to the list of new docs                       
                    docs_with_discover.append(self.discover(doc))
                docs=docs_with_discover
            # return the result as a resisearch result            
            return DotMap(total=result.total, docs=docs)
        except Exception as ex:
            raise rSearchException(str(ex), {'query':query})
    
    def paginate(self, query:str="*", page:int=1, num:int=10, sort_by:str='id', direction:bool=True, slop:int=0)->Pagination:
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


class Document(BaseDocument):
    
    class Definition(BaseDefinition):
        pass
         
    class AddForm(Definition):
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
            form_obj=use_form or self.Definition
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

class BasicDocument(Document):
    class Definition(BaseDefinition):
        description = StringField( 'Descripción', 
                                   validators = [validators.Length(max=50), validators.InputRequired()],
                                   render_kw=dict(indexed=True, on_table=True)) 

    class DeleteForm(Definition):
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
        self.delim='_' # do not use {:, /, #, ?} or anything related with url encoding

    def set_fk(self, definition:Document, depends_of: Document)->None:
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
                if (field.startswith('__') or field=='payload'):
                    continue
                fields.update({ field : getattr(doc, field) })            
            reslist.append(fields)
        return reslist

    def qescape(self, term:str)->str:
        """ escape redisearch characters. use it in search field values.
            https://oss.redislabs.com/redisearch/Escaping/        
        """
        if not term:
            return ''
        # chars:set=string.ascii_uppercase + string.ascii_lowercase + string.digits + '_'
        chars=(',','.','<','>','{','}','[',']','"', '\'', ':',';','!','@','#','$','%','^','&','*','(',')','-','+','=','~')
        t=''
        for g in term:
            if g in chars:
                t+='\\'+g
            else:
                t+=g
        return t

    def qunescape(self, term:str)->str:
        """ unescape redisearch characters. remove all backslash              
        """
        if not term:
            return ''
        return term.replace('\\', '')