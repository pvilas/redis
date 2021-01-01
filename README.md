# rDatabase

A very lightweight interface with RediSearch.

It implements **foreign keys and input validation via WTForms**.

You can **save**, **delete** and **query** documents.

```python
class Country(rBasicDocument):
    def __init__(self, db):
        """ a basic document already has id and description fields """
        super().__init__(db, 'COUNTRY')

class Persona(rWTFDocument):
    class AddForm(Form):
        id = StringField('Id', [validators.Length(min=3, max=50), validators.InputRequired()]) 
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

db=rTestDatabase(r)

print("Create some documents")
print(db.country.save(id="ES", description="España"))
print(db.country.save(id="FR", description="Francia"))
print(db.country.save(id="DE", description="Alemania"))
print(db.country.save(id="IT", description="Italia"))

print(db.persona.save(name="Manuel", country=db.k("COUNTRY","ES")))
print(db.persona.save(name="Hermman", country=db.k("COUNTRY","DE")))
print(db.persona.save(name="Pierre", country=db.k("COUNTRY","FR")))

"""
# list some data about persona refer RediSearch for query syntax
for p in persona.search("*", sort_by="name").docs:
    print(p.name, p.country)
"""

#uncomment this to raise an exception: the country PP does not exist
#print(db.persona.save(name="Pere", country=db.k("COUNTRY","PP")))

# delete a pais and try to insert a new persona with it -> it will raise an exception
#db.pais.delete(db.k("COUNTRY", "IT"))
#print(db.persona.save(name="Guiovani", country=db.k("COUNTRY","IT")))

# list personas
print(db.tabbed(db.persona.search("*", sort_by="name").docs))

db.persona.delete('PERSONA:00000002')
print("persona deleted")

print(db.tabbed(db.persona.search("*", sort_by="name").docs))
```

### Some info about Redis and RediSearch

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

Start Redis on startup:

```brew services start redis```

Or, if you don't want/need a background service you can just run:

```redis-server /usr/local/etc/redis.conf```

Executables can be found at:

```sh
/usr/local/bin/redis-cli
/usr/local/bin/redis-server
```

#### The RediSearch Tutorial

RediSearch [tutorial](https://github.com/RediSearch/redisearch-getting-started)
