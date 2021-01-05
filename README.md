# rDatabase - A very lightweight RediSearch interface with foreign keys and input validation

With rDatabase you can **validate**, **save**, **delete**, **query** and **paginate** documents. Moreover, it helps you to maintain the database **integrity**.  It uses the [**RediSearch**](https://oss.redislabs.com/redisearch/) module.

Other facilities are:

- Autodiscover first level of related documents via foreing keys, like in `customer.country.description`.
- Very easy to integrate with web apps as it uses WTForms to define and validate the documents.

```python
class Country(BasicDocument):
    """ A basic document already has and id and a description fields """
    pass

class Persona(Document):
    """ A document type called Persona """
    class Definition(BaseDefinition):
        """ Definition of persona using WTForms """
        name = StringField( label='Name', validators=[validators.Length(max=50), validators.InputRequired()], render_kw=dict(indexed=True, on_table=True))
        country = StringField( label='Country',
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

    # we provide the document id
    print(db.country.save(id="ES", description="España"))
    print(db.country.save(id="FR", description="Francia"))
    print(db.country.save(id="DE", description="Alemania"))
    print(db.country.save(id="IT", description="Italia"))

    # the document id is auto-created
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
```

## Installation

Be sure you have python>=3.7 with `python -V`, clone the project and

`pip install -r requirements.txt`

Run test

`python test.py`

## Document id and key

All the docuents are stored as hashs. The key of the hash and the id of the document is always the same. If you dont provide an id, **rDatabase** will create one using an autoincremented value. The key and the id is compound by `class_name+delimitator+id`. The `class_name+delimitator` is always prefixed to all ids if you do not provide it. This is true in the `get` and `save` operations.

To recover a single document you can use `get` with the id, that calls [`hgetall`](https://redis.io/commands/hgetall) or perform a search like `search(query="@id:class_name.id")`. In the first case, `hgetall` is extremelly fast, the second case is already fast but uses the redisearch module.

## Document format

To save a document, use a dict with simple values (strings, integer or float) like:

```python
country.save({'id': 'BEL', 'description':'Belgium'})
> COUNTRY.BEL
```

or

```python
country.save(id='SVN', description='Slovenia')
> COUNTRY.SVN
```

To recover documents use `get`, `search` or `paginate`. `get` return a single dict like

```python
country.get("RUS")
> {'id': 'RUS', 'description':'Russia'}
```

whereas `search` or `paginate` return a list of dicts,

```python
country.search(query="R*").total
> 4
country.search(query="R*").docs
> [{'id': 'REU', 'description':'Réunion'},
{'id': 'ROU', 'description':'Romania'},
{'id': 'RUS', 'description':'Russian Federation'},
{'id': 'RWA', 'description':'Rwanda'}]
```

In fact, the returned dicts are [DotMap](https://github.com/drgrib/dotmap) so you can access the data with dot syntax.

## Search documents

It is done via the `search`or `paginate` procedures. Check the [RediSearch](https://oss.redislabs.com/redisearch/master/Query_Syntax/) documentation for the syntax of the query. The signature of `search` is 

```python
def search(self, query:str, start:int=0, num:int=10, sort_by:str='id', direction:bool=True, slop=0)->list:
```

## Document definition

We define the document using a subclass named Definition inside the document declaration. The Definition has the fields (or members) of the document. BaseDefinition is, in fact, a [WTForm](https://wtforms.readthedocs.io/en/stable/forms/) with a field called id.

```python
class Persona(Document):
    """ A document type called Persona """
    class Definition(BaseDefinition):
        """ Definition of persona using WTForms """
        name = StringField( label='Name', validators=[validators.Length(max=50), validators.InputRequired()], render_kw=dict(indexed=True, on_table=True))
        country = StringField( label='Country',
                                validators=[validators.Length(max=50), validators.InputRequired()],
                                render_kw=dict(indexed=True, on_table=True, dependant=True))
```

It is worth to say that the document declaration is only a template to **validate** the input dict. When you recover a document, you recover a dict, not a document object. In same way, when you save a document, you are saving the dict that you pass with save, not the object itself.

## Index service

The document index is build according the definition the first time that a document is instantiated. The subsequent changes, like add or remove fields from the index are not serviced.

### Index definition

You define the index in the Definition declaration of the Document as in:

```python
class Persona(Document):
    class Definition(BaseDefinition):        
        name = StringField( label='Name', 
                            validators=[ validators.Length(max=50), 
                                         validators.InputRequired()], 
                            render_kw=dict(indexed=True, on_table=True)) 

        country = StringField( label='Country', 
                               validators=[ validators.Length(max=50), 
                                            validators.InputRequired()], 
                               render_kw=dict(indexed=True, on_table=True, dependant=True))
```

You can notice the `indexed` value in the render_kw. Also note that country is `dependant` of another document those type is called Country (via the name of the field, dont be confused with the label property).

The index is build when you instantiate for the first time a document of this type, as in:

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

### Install redisearch package

`pip install redisearch`

### Install Redis on OSX

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

`cp build/redisearch.so /usr/local/etc`

Add the loadmodule to conf:

`echo "loadmodule /usr/local/etc/redisearch.so" >> /usr/local/etc/redis.conf`

Start redis:

`brew services start redis`

Check for the search module:

`redis-cli module list`

If you don't want/need a background service you can just run:

`redis-server /usr/local/etc/redis.conf`

Executables can be found at:

```sh
/usr/local/bin/redis-cli
/usr/local/bin/redis-server
```

#### The RediSearch Tutorial

RediSearch [tutorial](https://github.com/RediSearch/redisearch-getting-started)
