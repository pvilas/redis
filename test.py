import redis
from rdatabase import rBasicDocument, rDocument, rDatabase, BaseDefDoc
from loguru import logger
from wtforms import Form, BooleanField, StringField, HiddenField, validators
from redisearch import Client, TextField, NumericField,\
                        TextField as DateField, TextField as DatetimeField,\
                        IndexDefinition, Query
from pprint import pprint

class Country(rBasicDocument):
    pass

class Persona(rDocument):
    class DefDoc(BaseDefDoc):        
        name = StringField('Name', validators=[validators.Length(max=50), validators.InputRequired()], render_kw=dict(indexed=True, on_table=True)) 
        country = StringField( 'Pais', 
                                validators=[validators.Length(max=50), validators.InputRequired()], 
                                render_kw=dict(indexed=True, on_table=True, dependant=True))


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
    r.flushdb()

    db=rTestDatabase(r)

    print("\nInformation about documents")
    db.country.info()
    db.persona.info()    

    print("Create some documents\n")
    
    print(db.country.save(id="ES", description="España"))
    print(db.country.save(id="FR", description="Francia"))
    print(db.country.save(id="DE", description="Alemania"))
    print(db.country.save(id="IT", description="Italia"))
    
    print(db.persona.save(name="Manuel", country=db.k("COUNTRY","ES")))
    print(db.persona.save(name="Hermman", country=db.k("COUNTRY","DE")))
    print(db.persona.save(name="Pierre", country=db.k("COUNTRY","FR")))

    # list some data about persona
    print("\nList personas, note the description of the country\n"+'-'*50)
    for p in db.persona.search("*", sort_by="name").docs:        
        print(p.name, p.country.description)


    print("\nTesting integrity mechanism...\n")

    # The country PP does not exist -> raise ex
    try:
        print("Saving persona with non existent country...")
        print(db.persona.save(name="Pere", country=db.k("COUNTRY","PP")))
    except Exception as ex:
        print(f"Saving with non existent foreign key raised an exception: {ex}")

    # delete a country and try to insert a new persona with it -> it will raise an exception
    try:        
        db.country.delete(db.k("COUNTRY", "IT"))
        print("\nSaving persona with a deleted foreign key...")
        print(db.persona.save(name="Guiovani", country=db.k("COUNTRY","IT")))
    except Exception as ex:
        print(f"Saving with non existent foreign key raised an exception: {ex}")

    # create a persona with a deliminator and an invalid character in the key -> the key will be sanitized
    print(f"\nSaving persona with a non-sanitized id  gúg.gg...")
    print(db.persona.save(id=" gúg.gg", name="Michael", country=db.k("COUNTRY","FR")))

    print(f"\nSaving persona with another non-sanitized id PERSONA. .ñ.xx .yy...")
    print(db.persona.save(id="PERSONA. .ñ.xx .yy", name="François", country=db.k("COUNTRY","FR")))

    # create a persona with an invalid key -> must raise an exception
    try:
        print("\nSaving with an invalid key...")
        print(db.persona.save(id="PERSONA..", name="Must raise ex", country=db.k("COUNTRY","FR")))
    except Exception as ex:
        print(f"Saving with an invalid key raised an exception: {ex}")

    print("\nCreating some countries...\n"+'-'*30)
    import dataset
    print("Created!")

    # test pagination 
    print("\nTesting pagination\n")    
    page=5
    num=10
    p=db.country.paginate(query="*", page=page, num=num, sort_by='description', direction=True)
    print(f"\nDocuments in country, page {page} of {int(p.total/num)}: {num} results out of {p.total}\n"+'-'*60)
    pprint(p.items)

    print("\nSome searches...")
    print("\nPersonas whose name starts with 'her'")
    pprint(db.persona.search(query="her*").docs)
    print("\nPersonas whose country is ES")
    pprint(db.persona.search(query=db.k("COUNTRY","ES")).docs)
    print("\nGet the country Colombia")
    pprint(db.country.get("COL"))
    print("\nGet the countries with the exact description Chile")
    pprint(db.country.search("@description:Chile").docs)

    exit(0)
