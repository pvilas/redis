import redis
from rdatabase import rBasicDocument, rWTFDocument, rDatabase
from loguru import logger
from wtforms import Form, BooleanField, StringField, HiddenField, validators
from redisearch import Client, TextField, NumericField,\
                        TextField as DateField, TextField as DatetimeField,\
                        IndexDefinition, Query


class Country(rBasicDocument):
    def __init__(self, db):
        super().__init__(db, 'COUNTRY')

class Persona(rWTFDocument):
    class AddForm(Form):
        id = StringField('ID', [validators.Length(min=3, max=50), validators.InputRequired()]) 
        name = StringField('Name', [validators.Length(max=50), validators.InputRequired()]) 
        country = StringField('Pais', [validators.Length(max=50), validators.InputRequired()]) 

    def __init__(self, db):
        super().__init__(db, 'PERSONA', 
            idx_definition= (                                
                TextField('id', sortable=True),                
                TextField('name', sortable=True),             
                TextField('country', sortable=True) # same name as the referenced document
                ))


class rTestDatabase(rDatabase):

    def __init__(self, r):
        super().__init__(r)

        # create definition documents
        self.country=Country(self)
        self.persona=Persona(self)

        # set dependencies
        # persona has a country
        self.set_fk(self.persona, self.country)


if __name__ == "__main__":

    # create redis conn 
    r=redis.Redis(
        host='localhost',
        decode_responses=True # decode all to utf-8
    )

    # WARNING!! this will delete all your data
    #r.flushdb()

    db=rTestDatabase(r)

    print("Create some documents")
    
    print(db.country.save(id="ES", description="España"))
    print(db.country.save(id="FR", description="Francia"))
    print(db.country.save(id="DE", description="Alemania"))
    print(db.country.save(id="IT", description="Italia"))
    

    print(db.persona.save(name="Manuel", country=db.k("COUNTRY","ES")))
    print(db.persona.save(name="Hermman", country=db.k("COUNTRY","DE")))
    print(db.persona.save(name="Pierre", country=db.k("COUNTRY","FR")))

    
    # list some data about persona
    print("\nSome personas\n"+'-'*30)
    for p in db.persona.search("*", sort_by="name").docs:
        print(p.name, p.country)

    #uncomment this to raise an exception: the country PP does not exist
    #print(db.persona.save(name="Pere", country=db.k("COUNTRY","PP")))

    # delete a pais and try to insert a new persona with it -> it will raise an exception
    #db.pais.delete(db.k("COUNTRY", "IT"))
    #print(db.persona.save(name="Guiovani", country=db.k("COUNTRY","IT")))

    # list personas, refer to RediSearch for query syntax
    print("\nPersonas tabbed list\n"+'-'*30)
    print(db.tabbed(db.persona.search("*", sort_by="name").docs))

    """
    db.persona.delete('PERSONA:00000002')
    print("persona deleted")

    print(db.tabbed(db.persona.search("*", sort_by="name").docs))
    """

    # test pagination 
    # run `python dataset.py` first to create the test dataset
    page=5
    p=db.country.paginate(query="*", page=page, num=10, sort_by='description', direction=True)
    print(f"\nItems of country, page {page}\n"+'-'*30)
    print(p.items)
    
    exit(0)
