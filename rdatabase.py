import redis
from datetime import datetime
from collections.abc import MutableMapping
import random
import string
from time import time
import binascii
from redisearch import Client, TextField, NumericField,\
                        TextField as DateField, TextField as DatetimeField,\
                        IndexDefinition, Query
import arrow
from loguru import logger
from wtforms import Form, BooleanField, StringField, HiddenField, validators
from werkzeug.datastructures import MultiDict
import tablib

ver="0.5"

class rDocumentException(Exception):
    def __init__(self, message="Error message", doc:dict={})->None:
        logger.warning(f"rDocumentException ({type(self).__name__}) {doc.get('id', 'no_id')}: {message}. El documento era {doc}.")
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


class rBaseDocument(object):
    def __init__(self, db, prefix:str, idx_definition=()):
        """
            # rBaseDocument
            A RediSearch document but without imput validation

            ## Param
            conn - Redis connection
            prefix - name of the document i.e. PERSONA
            idx_fields - list of index fields
                        i.e.
                        (TextField('id', sortable=True), 
                        TextField('dni', sortable=True), 
                        TextField('nombre', sortable=True), 
                        TextField('apellidos', sortable=True)), 

            ## Remarks
            After the index creation (first time) the index definition is no longer synced with 
            the database. You must maintain manually the changes on Redis or simply delete the
            index with: 
            
            ```> FT.DROPINDEX idx:movie```

            And let redis to recreate it. This is usually fast but can't be an option in a production environment.
        """
        self.db=db
        self.prefix=prefix.upper()
        self.idx=Client(f"idx{self.db.delim}{self.prefix}", conn=db.r)

        # a list of the foreign key to check before save a document
        self.foreigns=[] 

        # cream index damunt la taula, p.e. PERSONA:
        # saltarà excepció si ja existeix
        try:
            self.idx.create_index(
                idx_definition,
                definition=IndexDefinition(prefix=[f'{self.prefix}{self.db.delim}']))
        except Exception as ex:
            pass

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
                    raise rFKNotExists(f"El miembro {f.prefix.lower()} de {self.prefix} no existe en el docuento", doc)
                if not self.db.r.exists(doc.get(f.prefix.lower())):
                    raise rFKNotExists(f"El miembro {d.prefix}.{f.prefix.lower()}, con valor {doc.get(f.prefix.lower())}, no existe como clave foránea de {f.prefix.upper()}", doc)

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
            # si no hi ha camp d'index, el cream i el populam
            if doc.get('id', None) is None: 
                # el nom de sa clau acaba en _KEY
                NOM_COMPTADOR=f"{self.prefix.upper()}_KEY"        
                # miram si està el comptador de ids
                n=self.db.r.get(NOM_COMPTADOR)
                # print(f"compatdor es {NOM_COMPTADOR} = {n}")
                if n is None:
                    self.db.r.set(NOM_COMPTADOR, 1)
                    n=1      
                # print(f"n = {n}")              
                doc['id']=f'{n}'.rjust(8, '0') #f'{n:08}'
                self.db.r.incr(NOM_COMPTADOR)
            else: # si hi ha camp d'index, el sanitizam
                doc['id']=self.db.key_sanitize(doc['id'])

            # call before_save, can raise an exception
            doc=self.before_save(doc)        

            # si no hi ha camp de creacio, el cream i el populam
            if doc.get('created_at', None) is None:
                doc['created_at']=self.db.now()
            
            # el camp updated_on el populam sempre
            doc['updated_at']=self.db.now()

            # cream la clau
            NOM_CLAU = self.db.k(self.prefix, doc['id']) #f"{self.prefix.upper()}{self.db.delim}{doc['id']}"
            # print(f"La clau es {NOM_CLAU}")

            # salvam el diccionari
            self.idx.redis.hset(NOM_CLAU, mapping=doc)

            # cridam after save
            self.after_save(doc, NOM_CLAU)

            return NOM_CLAU
        except Exception as ex:
            logger.error(f"Database error while saving doc id {doc.get('id')}: {ex}")
            raise rSaveException(ex, doc)

    def before_delete(self, id:str)->None:
        """ check if we can delete this document 
            At this stage, we can delete if this document is not the key of a foreign key
            raise an Exception if not
            ## Param
            * id - is the complete id prefix:id
            ## Exception
            rBeforeDeleteException
        """         
        for d in self.db.dependants:
            # dependants està organitzat com p.e. (PERSONA, PAIS)
            # miram si la dependència s'aplica a aquest document
            if (self.prefix==d[1].prefix): # volem esborrar un pais i persona en depén
                print(f"{d[0].prefix} depén de {self.prefix}, comprovant si hi ha algun doc a {d[0].prefix} amb la clau {id}")
                cad=f'@{d[1].prefix.lower()}:{id}'
                print(f"La cadena de busqueda a {d[0].prefix} es {cad}")                
                if d[0].search(cad).total>0:
                    raise rDeleteFKException(f"No se puede borrar {id} de {self.prefix} porque hay documentos en {d[0].prefix} que tienen esta clave", {"id":id})

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
            return self.idx.search(q)
        except Exception as ex:
            raise rSearchException(str(ex), {'query':query})

class rWTFDocument(rBaseDocument):
    class AddForm(Form):
        id = StringField('Id', [validators.Length(min=2, max=50), validators.InputRequired()]) 
        description = StringField('Descripción', [validators.Length(max=50), validators.InputRequired()]) 
         
    class EditForm(AddForm):
        id = HiddenField()        

    class SearchForm(Form):
        pass

    def __init__(self, db, prefix:str, idx_definition=()):
        """
            # rWTFDocument
            És un document de RediSearch amb validació de l'imput usant WTForms

            AddForm, EditForm, DeleteForm, SearchForm són les fitxes per defecte per fer
            les operacions. 

            AddForm s'utilitza com a base per a les **validacions** de les dades.

            ## Param
            conn - sa connexió amb redis
            prefix - el nom de sa taula, passarà a majúscules, sense els dos punts
            idx_fields - llistat amb parell de (nom, tipus)
                        p.e.
                        (TextField('id', sortable=True), 
                        TextField('dni', sortable=True), 
                        TextField('nombre', sortable=True), 
                        TextField('apellidos', sortable=True)), 
        """
        super().__init__(db, prefix, idx_definition)

    def validate(self, doc:dict, use_form:Form=None)->dict:
        """ validate the imput using an WTForm """
        if not doc:
            raise rBeforeSaveException('No puede validarse un documento nulo')

        # create the addform with the doc
        try:
            form_obj=use_form or self.AddForm
            form=form_obj(doc)
            if form.validate():
                doc=form.data
                return doc
            else:
                raise rValidationException(f'Hay errores de validación: {form.errors}', doc)
        except Exception as ex:
            raise rBeforeSaveException(f"Error validando: {ex}", doc)


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
    def __init__(self, db, prefix):
        """ a document with id and description """
        super().__init__(db, prefix.upper(), 
            idx_definition= (                                
                TextField('id', sortable=True), 
                TextField('description', sortable=True)))

class rDatabase(object):

    def __init__(self, r):
        """ a redis database with a collection of descriptions """
        self.r = r
        self.dependants=[]
        self.delim='/' # do not use colon

    def set_fk(self, definition:rWTFDocument, depends_of: rWTFDocument)->None:
        self.dependants.append((definition, depends_of))        

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