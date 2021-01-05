# rDatabase - A very lightweight RediSearch interface with foreign keys and input validation

With rDatabase you can **validate**, **save**, **delete**, **query** and **paginate** documents with [**RediSearch**](https://oss.redislabs.com/redisearch/). Moreover, it helps you to maintain the database **integrity**.

Other facilities are:

* Autodiscover first level of related documents via foreing keys, like in `customer.country.description`.
* Very easy to integrate with web apps as we use WTForms to define and validate the documents.


```python
class Country(rBasicDocument):
    """ A basic document already has members id and description """
    pass

class Persona(rDocument):
    """ A document type called Persona """
    class DefDoc(BaseDefDoc):
        """ Definition of persona using WTForms """        
        name = StringField('Name', validators=[validators.Length(max=50), validators.InputRequired()], render_kw=dict(indexed=True, on_table=True)) 
        country = StringField( 'Pais', 
                                validators=[validators.Length(max=50), validators.InputRequired()], 
                                render_kw=dict(indexed=True, on_table=True, dependant=True))


class rTestDatabase(rDatabase):
    """ our database of documents """

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
    # r.flushdb()

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
        print(db.persona.save(name="Guiovanni", country=db.k("COUNTRY","IT")))
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

    exit(0)
```

## Installation

Be sure you have python>=3.7 with `python -V`, clone the project and

```pip install -r requirements.txt```

Run test

```python test.py```

## Delimitator

In **rDatabase** the key and the mandatory id of the document are the same.

If you are thinking on using the database with a web app, it could be better not to use http-related characters like `{?, :, /, #, ...}` as a separator. The reason is that you would use the id of the document in your http querys (as in `http://something.com/products/id_of_the_product`) and these characters will interfere if you dont encode it.

It is far better use something like `{., -, _, }`.

The default delimitator is `.`.


## Index service

The document index is build according the definition the first time that a document is instantiated. The subsequent changes, like add or remove fields from the index are not serviced. 

### Index definition

You define the index in the docuent declaration as in:

```python
class Persona(rWTFDocument):
    def __init__(self, db):        
        super().__init__(db, 'PERSONA', 
            idx_definition= (                                
                TextField('id', sortable=True),                
                TextField('name', sortable=True),             
                TextField('country', sortable=True) # same name as the referenced document
                ))
```

And the index is build when you instantiate for the first time the document, as in:

```python
class rTestDatabase(rDatabase):
    def __init__(self, r):
        super().__init__(r)

        self.persona=Persona(self)
```

But further changes like add or remove fields from the index (e.g. remove `name` from the index) are not posted automaticaly to the database. You must change the index manually.

### How to maintain the indices

One solution is to delete manually the index with `> FT.DROPINDEX idx_name` and it will be rebuild the first time a document of its class is instantiated.

If your database is in production, instead of deleting the index, it could be better to write manually the changes with [ft.alter schema add](https://oss.redislabs.com/redisearch/Commands/#ftalter_schema_add). You can get a report of the indices with `> FT._LIST` and information with`> FT.INFO idx_name`.


### Jinja html paginator example

To use the paginator, include `paginate.jinja` in your project and call it from your template with the paginator object as `{{render_pagination(p)}}`.

## Redis and RediSearch

#### Install redisearch package

```pip install redisearch```

#### Install Redis on OSX

```sh
brew install redis
```

Compile the RediSearch module (note the **recursive** param):

```sh
brew install cmake
cd $HOME/src
git clone --recursive https://github.com/RediSearch/RediSearch.git 
cd RediSearch
make
```

Copy the module at `/usr/local/etc`:

```cp build/redisearch.so /usr/local/etc```

Add the loadmodule to conf:

```echo "loadmodule /usr/local/etc/redisearch.so" >> /usr/local/etc/redis.conf```

Start redis:

```brew services start redis```

Check for the search module:

```redis-cli module list```

If you don't want/need a background service you can just run:

```redis-server /usr/local/etc/redis.conf```

Executables can be found at:

```sh
/usr/local/bin/redis-cli
/usr/local/bin/redis-server
```

#### The RediSearch Tutorial

RediSearch [tutorial](https://github.com/RediSearch/redisearch-getting-started)
