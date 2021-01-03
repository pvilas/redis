# rDatabase - A very lightweight RediSearch interface with foreign keys and input validation

With rDatabase you can **validate**, **save**, **delete**, **query** and **paginate** documents with Redis. Moreover, it helps you to maintain the database consistency.

```python
class Country(rBasicDocument):
    def __init__(self, db):
        """ a basic document already has id and description fields """
        super().__init__(db, 'COUNTRY')

class Persona(rWTFDocument):
    class AddForm(Form):
        """ validation Form for the Persona document """
        id = StringField('Id', [validators.Length(min=3, max=50), validators.InputRequired()]) 
        name = StringField('Name', [validators.Length(max=50), validators.InputRequired()]) 
        country = StringField('Country', [validators.Length(max=50), validators.InputRequired()]) 

    def __init__(self, db):
        """ refer to RediSearch documentation to know about index definition """
        super().__init__(db, 'PERSONA', 
            idx_definition= (                                
                TextField('id', sortable=True),                
                TextField('name', sortable=True),             
                TextField('country', sortable=True) # same name as the referenced document
                ))

class rTestDatabase(rDatabase):

    def __init__(self, r):
        """ create a database with two defined documents and their relationships """
        super().__init__(r)

        # create definition documents
        self.country=Country(self)
        self.persona=Persona(self)

        # set dependencies
        # persona has a country
        self.set_fk(self.persona, self.country)

db=rTestDatabase(r)

print("Validate and save some documents")
print(db.country.save(id="ES", description="España"))
print(db.country.save(id="FR", description="Francia"))
print(db.country.save(id="DE", description="Alemania"))
print(db.country.save(id="IT", description="Italia"))

print(db.persona.save(name="Manuel", country=db.k("COUNTRY","ES")))
print(db.persona.save(name="Hermman", country=db.k("COUNTRY","DE")))
print(db.persona.save(name="Pierre", country=db.k("COUNTRY","FR")))

"""
# list some data about persona. Refer RediSearch for query syntax
for p in db.persona.search("*", sort_by="name").docs:
    print(p.name, p.country)
"""

#uncomment next line to raise an exception: the country PP does not exist
#print(db.persona.save(name="Pere", country=db.k("COUNTRY","PP")))

# delete country ES -> it will raise an exception because there is more than zero Persona with this country
#db.country.delete(db.k("COUNTRY", "ES"))

# list personas
print(db.tabbed(db.persona.search("*", sort_by="name").docs))

db.persona.delete('PERSONA:00000002')
print("persona deleted")

# test pagination 
# run `python dataset.py` first to create the test dataset
page=5
p=db.country.paginate(query="*", page=page, num=10, sort_by='description', direction=True)
print(f"\nItems of country, page {page}\n"+'-'*30)
print(p.items)
```


### Pagination example with jinja2

You only need to define the following macro, taken from Flask-bootstrap, and call it with the paginator object as ```{{render_pagination(p)}}```.

```jinja
{% macro render_pagination(pagination,
                           endpoint=None,
                           prev=('&laquo;')|safe,
                           next=('&raquo;')|safe,
                           size=None,
                           ellipses='…',
                           args={},
                           fragment='',
                           align=''
                           )-%}
   
            <nav aria-label="Page navigation">
                <ul class="pagination{% if size %} pagination-{{ size }}{% endif %} {% if align == 'center' %}justify-content-center{% elif align == 'right' %}justify-content-end{% endif %}"{{ kwargs|xmlattr }}>
                    {# prev and next are only show if a symbol has been passed. #}
                    {% if prev != None -%}
                        <li class="page-item {% if not pagination.has_prev %}disabled{% endif %}">
                            <a class="page-link" data-page="{%if pagination.has_prev%}{{pagination.prev_num}}{%else%}#{%endif%}" href="#">{{ prev }}</a>
                        </li>
                    {%- endif -%}

                    {%- for page in pagination.iter_pages() %}
                        {% if page %}
                            {% if page != pagination.page %}
                                <li class="page-item">
                                    <a class="page-link" href="#" data-page="{{page}}">{{ page }}</a>
                                </li>
                            {% else %}
                                <li class="page-item active">
                                    <a class="page-link"  data-page="{{page}}" href="#">{{ page }} <span class="sr-only">(current)</span></a>
                                </li>
                            {% endif %}
                        {% elif ellipses != None %}
                            <li class="page-item disabled"><a class="page-link" href="#">{{ ellipses }}</a></li>
                        {% endif %}
                    {%- endfor %}

                    {% if next != None -%}
                        <li class="page-item {% if not pagination.has_next %}disabled{% endif %}">
                            <a class="page-link" data-page="{% if pagination.has_next%}{{pagination.next_num}}{%else%}#{%endif%}" href="#">{{ next }}</a>
                        </li>
                    {%- endif -%}
                </ul>
            </nav>
{% endmacro %}
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
