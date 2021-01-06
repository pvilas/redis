import redis
from rdatabase import BasicDocument, Document, rDatabase, BaseDefinition
from loguru import logger
from wtforms import Form, BooleanField, StringField, HiddenField, validators
from wtforms.fields.html5 import EmailField
from redisearch import Client, TextField, NumericField,\
                        TextField as DateField, TextField as DatetimeField,\
                        IndexDefinition, Query
from pprint import pprint
import arrow
from datetime import datetime
from dotmap import DotMap
from blessings import Terminal

class Country(BasicDocument):
    pass

class Person(Document):
    class Definition(BaseDefinition):        
        name = StringField('Name', validators=[validators.Length(max=50), validators.InputRequired()], render_kw=dict(indexed=True, on_table=True)) 
        country = StringField( 'Country', 
                                validators=[validators.Length(max=50), validators.InputRequired()], 
                                render_kw=dict(indexed=True, on_table=True, dependant=True))
        email = EmailField( 'Email', validators=[validators.Length(max=50)], 
                                render_kw=dict(unique=True))


class rTestDatabase(rDatabase):

    def __init__(self, r):
        super().__init__(r)

        # create definition documents
        self.country=Country(self)
        self.person=Person(self)

        # set dependencies
        # person has a country
        self.set_fk(self.person, self.country)


if __name__ == "__main__":

    t = Terminal() # ansi terminal

    def title(s:str):        
        print(f"\n{t.bold}{s}\n"+'-'*len(s)+f'{t.normal}')

    def subtitle(s:str):        
        print(f"\n{t.blue}{t.bold}{s}{t.normal}")

    def subsubtitle(s:str):        
        print(f"\n{t.green}{s}{t.normal}")

    def p(s:str):        
        print(f"{t.cyan}{s}{t.normal}")

    def err(s:str):        
        print(f"{t.red}{s}{t.normal}")


    # create redis conn 
    r=redis.Redis(
        host='localhost',
        decode_responses=True # decode all to utf-8
    )

    # WARNING!! this will delete all your data
    r.flushdb()

    db=rTestDatabase(r)

    title("Information about documents")
    db.country.info()
    db.person.info()    

    subtitle("Create some documents")
    
    p(db.country.save({'id': "ES", 'description':"España"}))

    p(db.country.save(dict(id="FR", description="Francia")))
    p(db.country.save(dict(id="DE", description="Alemania")))
    p(db.country.save(dict(id="IT", description="Italia")))
    p(db.country.save(dict(id="FI", description="Finlándia")))

    p(db.person.save(dict(name="Manuel", country=db.k("COUNTRY","ES"))))
    p(db.person.save(dict(name="Hermman", country=db.k("COUNTRY","DE"))))
    p(db.person.save(dict(name="Pierre", country=db.k("COUNTRY","FR"))))
    p(db.person.save(dict(name="Linux", country=db.k("COUNTRY","FI"))))

    # list some data about person
    subtitle("List persons, note the description of the country")
    for d in db.person.search("*", sort_by="name").docs:        
        p(f"{d.name}, {d.country.description}")

    title("Testing integrity mechanism")

    # The country PP does not exist -> raise ex
    try:
        subtitle("Saving person with non existent country...")
        p(db.person.s(name="Pere", country=db.k("COUNTRY","PP")))
    except Exception as ex:
        err(f"Saving with non existent foreign key raised an exception: {ex}")

    # delete a country and try to insert a new person with it -> it will raise an exception
    try:        
        db.country.delete(db.k("COUNTRY", "IT"))
        subtitle("Saving person with a deleted foreign key...")
        p(db.person.s(name="Guiovani", country=db.k("COUNTRY","IT")))
    except Exception as ex:
        err(f"Saving with non existent foreign key raised an exception: {ex}")

    # create a person with a deliminator and an invalid character in the key -> the key will be sanitized
    subtitle(f"Saving person with a non-sanitized id  gúg.gg...")
    p(db.person.s(id=" gúg.gg", name="Michael", country=db.k("COUNTRY","FR")))

    subtitle(f"Saving person with another non-sanitized id person. .ñ.xx .yy...")
    p(db.person.s(id="person. .ñ.xx .yy", name="François", country=db.k("COUNTRY","FR")))

    # create a person with an invalid key -> must raise an exception
    try:
        subtitle("Saving with an invalid key...")
        p(db.person.s(id="person..", name="Must raise ex", country=db.k("COUNTRY","FR")))
    except Exception as ex:
        err(f"Saving with an invalid key raised an exception: {ex}")

    title("Creating some countries")
    import dataset
    p("Created!")

    # test pagination 
    title("Testing pagination")    
    page=5
    num=10
    d=db.country.paginate(query="*", page=page, num=num, sort_by='description', direction=True)
    subtitle(f"Documents in country, page {page} of {int(d.total/num)}: {num} results out of {d.total}")
    pprint(d.items)

    title("Some searches")
    subsubtitle(f"persons whose name starts with 'her'")
    pprint(db.person.search(query="her*").docs)
    subsubtitle("persons whose country is ES")
    pprint(db.person.search(query=db.k("COUNTRY","ES")).docs)
    subsubtitle("Get the country Colombia")
    pprint(db.country.get("COL"))
    subsubtitle("Get the countries with the exact description Chile")
    pprint(db.country.search("@description:\"Chile\"").docs)

    title("Update documents")
    subtitle("Find out a person named Linux")
    l=db.person.search(query="Linux").docs[0]    
    pprint(l)
    subsubtitle("Ups.... The name is mispelled. Correcting to Linus ...")
    l.name="Linus"
    k=db.person.save(l)
    # save always return the id of the saved document
    subsubtitle("The updated document is")
    pprint(db.person.get(k))

    title("Test uniqueness")
    p(db.person.s(name="Pere", country=db.country.k("NAM"), email="hola@nam.com"))
    p(db.person.s(name="Josep", country=db.country.k("TCA"), email="hola1@nam.com"))
    try:
        p(db.person.s(name="Josep", country=db.country.k("TCA"), email="hola@nam.com"))
    except Exception as ex:
        err(f"Saving with an duplicated unique key raised an exception: {ex}")

    subsubtitle("Who has the duplicated email?")
    q=f"hola\@nam\.com"
    p(f"Searching {q}")
    pprint(db.person.search(q).docs)

    exit(0)
