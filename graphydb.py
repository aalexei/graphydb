#!/usr/bin/env python
#
# GraphyDB
#
# A python graph database implemented on top of SQLite.
#
# Alexei Gilchrist
# Copyright 2021
# 
# (Python 3 required)
#  
#
'''
# Overview

GraphyDB is a graph database for Python 3 built ontop of SQLite.

There are many cases where a graph structure is a better fit to a problem domain than a set of tables.
GraphyDB was designed to fill a niche where a flexible embedded graph database was needed
for a moderate sized problem (~10,000 edges and nodes). GraphyDB is not designed to handle terrabytes
of data, and has not been particularly optimised for speed.


# Quick start

For example. let's instantiate a graph in memory and add some nodes then connect them, saving them immediately.

    from graphydb import Graph

    g = Graph()
    
    anne = g.Node('Person', name="Anne").save()
    bob = g.Node('Person', name="Bob", nickname='Bobby').save()
    charlie = g.Node('Person', name="Charlie").save()
    
    coffee = g.Node('Drink', sort="Coffee").save()
    tea = g.Node('Drink', sort="Tea").save()
    
    g.Edge(anne, 'Likes', bob).save()
    g.Edge(charlie, 'Likes', bob).save()
    
    g.Edge(anne, 'Drinks', coffee, strength='weak').save()
    g.Edge(charlie, 'Drinks', coffee, strength='weak').save()

Now we can find who drinks coffee. If we have the node we can fetch the incoming references

    p1=coffee.inN('e.kind = "Drinks"')
    
    > {[(Y7YQHVNCVUZ9AHH2YH3UVIH86:Person), (3ZKZI0PQAF3CNMEQ7WLUVTW6F:Person)]}
    
Or we can query the database directly (and build more sophisticated queries)

    p2=g.fetch('[p:Person,strength] -(e:Drinks)> (d:Drink)', 'd.data.sort = "Coffee"', strength='e.data.strength')
    
    > {[(Y7YQHVNCVUZ9AHH2YH3UVIH86:Person), (3ZKZI0PQAF3CNMEQ7WLUVTW6F:Person)]}
    
    p2[0].data
    
    > {'_strength': 'weak',
      'ctime': 1474270482.224738,
      'kind': 'Person',
      'mtime': 1474270482.224739,
      'name': 'Anne',
      'uid': 'Y7YQHVNCVUZ9AHH2YH3UVIH86'}
    

# SQLite structure    

Two tables hold most of the data, one for nodes and one for edges. 
Additional tables provide a key-value stores for preferences and a cache. FTS indices are
also held in the database.


## Nodes

Nodes are held in the table `nodes` with the columns

- `uid` [TEXT PRIMARY KEY] A 25 character UUID assumed to be unique across all items past and future
- `kind` [TEXT] The node kind, e.g. "Person", "Document" etc
- `ctime` [REAL] Item creation time in seconds since the epoch as a floating point number
- `mtime` [REAL] Item last modification time in seconds since the epoch as a floating point number
- `data` [TEXT] A JSON encoded distionary of keys and values

## Edges

Edges are held in the table `edges` with the columns

- `uid` [TEXT PRIMARY KEY] A 25 character UUID assumed to be unique across all items past and future
- `kind` [TEXT] The edge kind e.g. "Likes", "Authored" etc
- `startuid` [TEXT NOT NULL REFERENCES nodes(uid)]
- `enduid` [TEXT NOT NULL REFERENCES nodes(uid)]
- `ctime` [REAL] Item creation time in seconds since the epoch as a floating point number
- `mtime` [REAL] Item last modification time in seconds since the epoch as a floating point number
- `data` [TEXT] A JSON encoded distionary of keys and values

Note that any two nodes can be connected by multiple edges so the structure is not a simple graph but
a directed multigraph with the possibility of loops.
This makes it possible to have metadata associated with each edge kind. It's up to the application to
deal with multiple edges.

## Additional tables

Two additional tables `settings` and `cache` provide simple key-value stores with the columns

- `key` [TEXT PRIMARY KEY] Some unique string for the key
- `value` [TEXT] JSON encoded data for the value

# Installing

## Dependencies

  1. apsw (with fts5 and json1 extensions)
  
  
# Module details
'''

import json, re, os, random, fnmatch, time, copy
from collections import MutableMapping
import apsw
import logging
from datetime import datetime
import functools, itertools

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

__version__ = 0.42


RESERVED = ['uid','kind','ctime','mtime','startuid','enduid']
'''Reserved keyword that cannot be used in node and edge data.'''

FETCHKEYWORDS = ['WHERE','CHAIN','ORDER','LIMIT','GROUP', 'COUNT', 'DISTINCT', 'OFFSET', 'DEBUG']
'''Keywords used in `graphydb.Graph.fetch`, everything else is a parameter.'''

#-------------------------------------------------------------------------------- 
def generateUUID():
    '''
    Generate a random UUID.
    Make as short as possible by encoding in all numbers and letters.
    Sequence has to be case insensitive to support any filesystem and web.
    '''
    ## the standard uuid is 16 bytes. this has
    ## 256**16 = 340282366920938463463374607431768211456 possible values
    ## In hex with the alphabet '0123456789abcdef' this is
    ## 16**32 = 340282366920938463463374607431768211456
    ## encoding with the alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    ## can be done in 25 characters:
    ## 36**25 = 808281277464764060643139600456536293376
    ## keep case insensitive for robustness in URLS etc 
    ## (case sensitivity would only drop it to 22 characters)

    alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    N = len(alphabet)

    # emulate how uuid4 grabs entropy
    try:
        # first try to use the system urandom module if available
        # this should be more cryptographically secure than random
        rand = random.SystemRandom().random
        uu = ''.join([alphabet[(rand()*N).__int__()] for i in range(25)])
    except:
        # fall back on random
        rand = random.random
        uu = ''.join([alphabet[(rand()*N).__int__()] for i in range(25)])

    return uu

#--------------------------------------------------------------------------------
def jsonextract(param):
    '''
    Helper function to wrap json extractions.
       
        e.g. `x.data.y` becomes `json_extract(x.data, "$.y")`

    '''
    return re.sub('(\w+)\.data\.(\w+)',r'json_extract(\1.data, "$.\2")', param)

def ensurelist(x):
    '''
    Helper function to ensure argument is a list.
    '''
    if x is None:
        x = []
    elif type(x) != type([]):
        x = [x]
    return x

def conditionalyield(keys,A,B):
    '''
    Iterator over values A[k] | B[k]
    '''
    for k in keys:
        yield (A[k] if k in A else B[k])
        
def cleandata(fulldata):
    '''
    Return dict without keys that start with underscore (which are treated as temporary local variables).
    '''
    data = {k:v for k,v in fulldata.items() if k[0] != '_'}    
    return data

def diff(d1,d2,changedkeys):
    '''
    Calculate a simple diff that takes dict d1 to d2.
    Only keys in the set changedkeys are considered.
    Keys starting with underscore are ignored.
    '''
    remove = {}
    add = {}
    
    for k in d1.keys()|d2.keys():
        if k[0] == '_':
            continue
        elif k in changedkeys:
            ## only consider keys explicitly marked as changed
            if k not in d2:
                remove[k] = d1[k]
            elif k not in d1:
                add[k] = d2[k]
            elif d1[k]!=d2[k]:
                ## only stored if values are actually different
                remove[k] = d1[k]
                add[k] = d2[k]
            
    if len(remove) == 1 and 'mtime' in remove and len(add) == 1 and 'mtime' in add:
        remove = {}
        add = {}
        
    change = {}
    if len(add)>0:
        change['+'] = add
    if len(remove)>0:
        change['-'] = remove
        
    return change

def patch(d, change, reverse=False):
    '''
    Patch a dict based on a change dict.
    Return a patched shallow copy.
    '''
    d2=dict(d)
    if reverse:
        for k in change.get('+',{}).keys():
            del d2[k]
        d2.update(change.get('-',{}))
    else:
        for k in change.get('-',{}).keys():
            del d2[k]
        d2.update(change.get('+',{}))
    return d2
#-------------------------------------------------------------------------------- 
class GraphyDBException(Exception):
    '''
    Any exceptions thrown by `graphydb`.
    '''
    pass

#--------------------------------------------------------------------------------
class IndexedSet:
    '''
    Implements an indexed and sorted set.
    
    The collection supports a subset of list, set, and dict operations. 
    
    The objects in the collection must expose a `__uid__()` method that returns a unique string uid 
    for the object. This uid is what will be used to index the object and in set comparisons.
    
    Items are maintained in order and are indexed so can be looked up by uid. Internally, the data is 
    stored in a dict `_index` *and* list `_list`, but these shouldn't be modified directly as
    they need to be kept in sync.
    
    Speed of set operations are about 10x slower than native sets but with a much faster
    creation time for populating the collection. Since set operations are already really fast, 
    the collection has been optimised to reduce the creation time to have overall performance.
    '''
        
    def __init__(self, iterable=[]):
        '''
        Takes an interable of objects with a `__uid__()` method. 
        '''
        self._index = {n.__uid__():n for n in iterable}
        self._list = list(iterable)
        if len(self._list) != len(self._index.keys()):
            ## iterable contains duplicates. Base the list on the _index.
            self._list = list(self._index.values())

    def copy(self):
        '''
        Return a shallow copy. 
        
        This means any mutable objects inside the 
        collected object with be references to the original.
        '''
        ## N.B. in __init__ a shallow copy is made anyway  
        ## but it's faster to copy the parsed structures
        new = self.__class__()
        new._index = self._index.copy()
        new._list = self._list.copy()
        return new

    #
    # list methods
    #
    
    def sort(self, key=None, reverse=False):
        '''
        Sort items in place. Returns reference.
        '''
        self._list.sort(key=key, reverse=reverse)
        return self
        
    def __getitem__(self, key):
        if isinstance(key, slice):
            return self.__class__(self._list[key])
        elif isinstance(key, str):
            return self._index[key]
        else:
            return self._list[key]
        
    def __iter__(self):  
        return iter(self._list)
    
    def reverse(self):
        '''
        Reverse item order in place. Returns reference.
        '''
        self._list.reverse()
        return self

    def __delitem__(self, i):
        if isinstance(i, slice):
            values = self._list[i]
        else:
            values = [self._list[i]]
        for v in values:
            del self._index[v.__uid__()]
        del self._list[i]
    
    def __repr__(self):
        return "{{{}}}".format(self._list.__repr__()) 

    def append(self, item):
        '''
        Append an item to collection, 
        overwriting and moving to end if present (by uid).
        Returns reference.
        '''
        self.discard(item)
        uid = item.__uid__()
        self._index[uid] = item
        self._list.append(item)  
        return self
    
    #
    # set methods
    #
    
    def clear(self):
        '''
        Clear all the contents. Returns reference.
        '''
        self._list = list()
        self._index = dict()
        return self
    
    def add(self, item):
        '''
        Add an item to collection, 
        overwriting if already present (by uid) and keeping position.
        Returns reference.
        '''
        uid = item.__uid__()
        if uid in self._index:
            current = self._index[uid]
            self._index[uid] = item
            idx = self._list.index(current)
            self._list[idx] = item
        else:
            self._list.append(item)
            self._index[uid]=item
        return self

    def remove(self, item):
        '''
        Remove item (with same uid) from the collection.
        Raise KeyError if item not present.
        Returns reference.
        '''
        uid = item.__uid__()
        ## make sure it is the item in collection with same uid
        actualitem = self._index[uid]
        self._list.remove(actualitem)
        del self._index[uid]
        return self
     
    def discard(self, item):
        '''
        Remove item (with same uid) from the collection.
        Ignore if item not present.
        Returns reference.
        '''
        uid = item.__uid__()
        if uid in self._index:
            ## make sure it is the item in collection with same uid
            actualitem = self._index[uid]
            self._list.remove(actualitem)
            del self._index[uid]
        return self
    
    def __lt__(self, other):
        return self._index.keys().__lt__(other._index.keys())
    def __le__(self, other):
        return self._index.keys().__le__(other._index.keys())
    def __eq__(self, other):
        return self._index.keys().__eq__(other._index.keys())
    def __ne__(self, other):
        return self._index.keys().__ne__(other._index.keys())
    def __gt__(self, other):
        return self._index.keys().__gt__(other._index.keys())
    def __ge__(self, other):
        return self._index.keys().__ge__(other._index.keys())
    def __cmp__(self, other):
        return self._index.keys().__cmp__(other._index.keys())

    def union(self, *others):
        return functools.reduce(lambda x,y:x|y,others, self) 
    def intersection(self, *others):
        return functools.reduce(lambda x,y:x&y,others, self)
    def difference(self, *others):
        return functools.reduce(lambda x,y:x-y,others, self)   

    def symmetric_difference(self, other):
        ## N.B. keys() has no symmetric_difference() so convert to full set first
        keys = set(self._index.keys()).symmetric_difference(other._index.keys())
        return self.__class__(conditionalyield(keys,self._index,other._index))

    def __and__(self, other):
        keys = self._index.keys().__and__(other._index.keys())
        return self.__class__(conditionalyield(keys,self._index,other._index))
    def __xor__(self, other):
        keys = self._index.keys().__xor__(other._index.keys())
        return self.__class__(conditionalyield(keys,self._index,other._index))
    def __or__(self, other):
        keys = self._index.keys().__or__(other._index.keys())
        return self.__class__(conditionalyield(keys, self._index, other._index))
    def __sub__(self, other):
        keys = self._index.keys().__sub__(other._index.keys())
        return self.__class__(conditionalyield(keys, self._index, other._index))
            
    #
    # common methods
    #
       
    def __len__(self):
        return self._index.__len__()
    
    def __contains__(self, item):
        '''
        Based on uid only.
        '''
        return self._index.__contains__(item)
    
    def pop(self, idx=-1):
        '''
        Retrieves the item at location `idx` and also removes it. Defaults to end of list.
        '''
        item = self._list.pop(idx)
        del self._index[item.__uid__()]
        return item
           
    def update(self, *iterables):
        '''
        Uodate the existing items with the items in `*iterables`.
        Returns reference.
        '''
        _add = self.add
        for iterable in iterables:
            for value in iterable:
                _add(value)
        return self
    
#-------------------------------------------------------------------------------- 
class Graph:
    '''
    A graph composed of nodes and edges, both stored in SQLite database.
    '''
    def __init__(self, path=':memory:'):
        '''
        Instantiating it without argument creates an in-memory database, 
        pass in a path to create or open a database in a file
    
            memdb = Graph()
       
            filedb = Graph(path)
        '''
        self.path = path
        if os.path.exists(path):
            ## connect to existing database
            self.connection = apsw.Connection(self.path)
        else:
            ## create new database and set up tables
            self.connection = apsw.Connection(self.path)
            self.reset() 
            self.resetfts()
        
    def reset(self):
        '''
        Drop the tables and recreate them.
        *All data will be lost!*
        '''
        cursor=self.cursor()
                  
        cursor.execute('''
            DROP TABLE IF EXISTS nodes;
            DROP TABLE IF EXISTS edges;
            DROP TABLE IF EXISTS settings;
            DROP TABLE IF EXISTS cache;
            DROP TABLE IF EXISTS changes;
            CREATE TABLE IF NOT EXISTS nodes(uid TEXT PRIMARY KEY, kind TEXT, ctime REAL, mtime REAL, data TEXT);
            CREATE TABLE IF NOT EXISTS edges(uid TEXT PRIMARY KEY, kind TEXT, startuid TEXT NOT NULL REFERENCES nodes(uid), enduid TEXT NOT NULL REFERENCES nodes(uid), ctime REAL, mtime REAL, data TEXT);
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS cache(key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS changes(id INTEGER PRIMARY KEY AUTOINCREMENT, change TEXT);
        ''')
        
        ## store GraphyDB version that was used to create the database
        self.savesetting('GraphyDB version', __version__)

    def countchanges(self):
        cursor=self.cursor()
        n=cursor.execute('SELECT COUNT(*) FROM changes').fetchone()[0]
        return n
    
    def clearchanges(self):
        ## recreate table so it resets the IDs
        cursor=self.cursor()
        cursor.execute('''
        DROP TABLE IF EXISTS changes;
        CREATE TABLE changes(id INTEGER PRIMARY KEY AUTOINCREMENT, change TEXT);
        VACUUM;
        ''')        
    
    def lastchanges(self):
        if self.countchanges()==0:
            ## no changes
            out = []
        else:
            cursor=self.cursor()
            cid, change = cursor.execute('''
                SELECT id, change FROM changes
                ORDER BY id DESC LIMIT 1
                ''').fetchone()
            change = json.loads(change)
            if 'batch' not in change:
                ## single change item
                out = [(cid, change)]
            else:
                ## possibly multiple change items in same batch
                rows = cursor.execute('''
                    SELECT id, change FROM changes
                    WHERE json_extract(change, "$.batch") = ? ORDER BY id''', [change['batch']]).fetchall()   
                out = [(cid, json.loads(change)) for cid, change in rows]
                
        return out
    
    def deletechange(self, id):
        cursor=self.cursor()
        cursor.execute('DELETE FROM changes WHERE id = ?', [id])
        
    def addchange(self, new=None, old=None, batch=None):
        
        if new is None and old is None:
            return
            
        change = {}
        if new is None:
            ## this is a delete
            change['uid'] = old['uid']
            change['-'] = cleandata(old.data)
        elif old is None:
            ## this is add
            change['uid'] = new['uid']
            change['+'] = cleandata(new.data)
        else:
            ## item internals have changed
            d = diff(old.data, new.data, new._changedkeys)
            if len(d) == 0:
                return
            
            change['uid'] = new['uid']
            change.update(d)
        
        change.setdefault('time', time.time())
        change.setdefault('rev', generateUUID())
        if batch is not None:
            change['batch'] = batch
            
        change = json.dumps(change)
            
        cursor=self.cursor()
        row=cursor.execute('''INSERT INTO changes (change) VALUES (?)''', [change])

    def undo(self):
        '''
        Undo the last change to the graph.
        '''
        changes = []
        changebatch=reversed(self.lastchanges())
        for i, change in changebatch:
            if '+' in change and '-' not in change:
                ## change was to add item so undo removes it
                action = "-"
                item = self.getuid(change['uid'])
                item.delete(setchange=False) 
            elif '-' in change and '+' not in change:
                ## change was to remove item so undo adds it
                action = "+"
                data = change['-']
                if 'startuid' in data:
                    item = Edge(data, graph=self)
                else:
                    item = Node(data, graph=self)
                item.save(setchange=False)
            elif '-' in change and '+' in change:
                ## change was to add and remove internals so undo reverses them
                action = "*"
                item = self.getuid(change['uid'])
                item.data = patch(item.data, change, reverse=True)
                item.save(setchange=False, force=True)
            else:
                raise GraphyDBException('Unknown undo action')
            changes.append((action, change['uid']))
            self.deletechange(i)
        return changes

    def resetfts(self, nodefields=None, edgefields=None):
                
        ## remove tables
        cursor=self.cursor()
        cursor.execute('''
            DROP TABLE IF EXISTS nodefts;
            DROP TABLE IF EXISTS edgefts;
        ''') 
        
        ## create node table
        if nodefields is not None:
            nodefields = set(nodefields)
            VSTR = ",".join(nodefields) + ",uid UNINDEXED"
            cursor.execute('CREATE VIRTUAL TABLE IF NOT EXISTS nodefts USING fts5({});'.format(VSTR))
            
            
        ## create edge table
        if edgefields is not None:
            edgefields = set(edgefields)
            ESTR = ",".join(edgefields)+",uid UNINDEXED"
            cursor.execute('CREATE VIRTUAL TABLE IF NOT EXISTS edgefts USING fts5({});'.format(ESTR))
        
    def getsetting(self, key, default=None):
        '''
        Read back a previously saved setting. Value will be de-jsonified.
        '''
        cursor=self.cursor()
        row = cursor.execute('SELECT value FROM settings WHERE key = ?',[key]).fetchone()
        if row is None:
            return default
        
        value = json.loads(row[0])
        return value
        
    def savesetting(self, key, value):
        '''
        A simple key-value store to save settings. Values will be jsonified.
        '''
        cursor=self.cursor()
        settings = cursor.execute('INSERT OR REPLACE INTO settings(key, value) VALUES(?,?)', (key, json.dumps(value)) )

    def cached(self, key):
        '''
        Read back a previously cached item. Value will be de-jsonified.
        '''
        cursor=self.cursor()
        row = cursor.execute('SELECT value FROM cache WHERE key = ?',[key]).fetchone()
        if row is None:
            raise KeyError
        return json.loads(row[0])
        
    def cache(self, key, value):
        '''
        A simple key-value store to serve as a cache. Values will be stored jsonified under the given key.
        '''
        cursor=self.cursor()
        settings = cursor.execute('INSERT OR REPLACE INTO cache(key, value) VALUES(?,?)', (key, json.dumps(value)) )

    def cursor(self):
        '''
        Return an APSW cursor.
        
        This can be used to excute SQL queries directly on the database.
        '''
        return self.connection.cursor()
    
    @property
    def stats(self):
        '''
        Return basic stats of the graph such as the number of edges and nodes.
        '''
        cursor=self.cursor()
        Nn = cursor.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]
        Ne = cursor.execute('SELECT COUNT(*) FROM edges').fetchone()[0]
        
        nkinds = {}
        for k,n in cursor.execute('SELECT kind, COUNT(kind) FROM nodes GROUP BY kind'):
            nkinds[k]=n
        ekinds = {}
        for k,n in cursor.execute('SELECT kind, COUNT(kind) FROM edges GROUP BY kind'):
            ekinds[k]=n
        
        S = {"Total nodes":Nn, "Total edges":Ne, "Node kinds":nkinds, "Edge kinds":ekinds}
        
        if self.path!=':memory:':
            stat = os.stat(self.path)
            size = stat.st_size
            if size < 1000:
                sizestr = "%dB"%size
            elif size < 1000000:
                sizestr = "%dK"%(size/1000)
            else:
                sizestr = "%dM"%(size/1000000)
            S['File size']= sizestr
            
        sversion = cursor.execute('SELECT sqlite_version()').fetchone()[0] 
        S['SQLite version'] = sversion
        S['GraphyDB version'] = self.getsetting('GraphyDB version')
        
        S['Changes'] = self.countchanges()
        
        return S

    def _parsechain(self, CHAIN, PARAM):
        '''
        Break down the chain of edges and nodes.
        '''
            
        aliases = {}
        collect = None
        left = None
        search1 = re.compile('\(([\w:]+)\)')
        search2 = re.compile('\[([\w:,]+)\]')
        for p in CHAIN.split():
            ## parse kind of item
            if p[-1] == '>':
                item = {'type':'right','table':'edges','leftuid':'startuid','rightuid':'enduid','ftstable':'edgefts','columns':['data']}
            elif p[0] == '<':
                item = {'type':'left','table':'edges','leftuid':'enduid','rightuid':'startuid','ftstable':'edgefts','columns':['data']}
            else:
                item = {'type':'node','table':'nodes','leftuid':'uid','rightuid':'uid','ftstable':'nodefts','columns':['data']}
                
            ## parse aliases, extra parameters and kinds
            so1=search1.search(p)
            so2=search2.search(p)
            if so1:
                tmp = so1.group(1).split(':')
                alias=tmp[0]
                if len(tmp)==2:
                    item["kind"]=tmp[1]
    
            elif so2:
                s = so2.group(1).split(",")
                tmp = s[0].split(':')
                alias=tmp[0]     
                collect = item
                if len(s)>1:
                    item['extra'] = {}
                    for c in s[1:]:
                        try:
                            col = '{} AS "{}"'.format(PARAM[c],c)
                        except KeyError:
                            raise GraphyDBException('Item "{}" not given an expansion'.format(c))
                        item['extra'][c]=col
                        ## remove these extra columns from parameters
                        del PARAM[c]
                if len(tmp)==2:
                    item["kind"]=tmp[1]

            else:
                raise GraphyDBException("Error in parsing format: '{}'".format(p) )
            
            if alias in aliases:
                raise GraphyDBException("Aliases must be unique ({} multiply defined)".format(alias) )
            
            
            item['alias'] = alias
            
            ## link
            if left is not None:
                item['leftlink'] = left['alias']
                left['rightlink'] = item['alias']   
                
            aliases[alias]=item
            left = item
        
        if collect is None:
            collect = item
    
        return aliases, collect
    
    def fetch(self, CHAIN='(n)', WHERE=None, **args):
        '''
        This is the workhorse for fetching nodes and edges from the database. It's a thin wrapper around
        SQL so most of the SQL operators are available.
        
        **Keywords**
        
        - `CHAIN`: Description of how to join together nodes and edges for the query. 
                   A chain is composed of links read from left to right separated by spaces. 
                   Each link can be a node "(n)" or and edge "-(e)>" or "<(e)-". 
                   e.g. "(n1) -[e:Document,title]> (n2)".
                   The variable in the brackets is an alias for the link that can then be used 
                   in other parts of the query and should be unique. 
                   Square brackets indicate the link to be collected (otherwise defaults to right-most link).
                   Square brackets can also have other aliases separated by commas, these should be defined in parameters passed
                   to the function.
        - `WHERE`: A string, or list of strings with SQL conditions. If it's a list the items will be ANDed together
        - `GROUP`: String to follow SQLs GROUP BY
        - `ORDER`: String to follow SQLs ORDER BY
        - `LIMIT`: An interger to limit the numer of items returned
        - `OFFSET`: Return items from offset, used in combination with `LIMIT`
        - `COUNT`: The number of items satisfying the query will be returned
        - `DISTINCT`: Distinct uids will be collected. [Defaults to `True`]
        - `DEBUG`: If this is set to `True` the generated SQL and parameters will be returned without making the query.
        
        For convenience `CHAIN` and `WHERE` are the first two implicit parameters.
        
        **Parameters**
        
        Every other keyword is treated as a parameter for defining returned values, FTS searches or SQL escaped parameters. 
        
        Any extra aliases in the collected item should be defined as a parameter. The result will be available as a key 
        in the item with the alias preceded by an underscore (i.e. an unsaved value). 
        
        If a parameter is the same as a link-alias with "_fts" appended then the value is to be
        used in an FTS match. 
        
        Values to be SQL escaped whould be inserted by name (e.g. ':p1') where appropriate and the value given by a parameter
        (e.g. p1=10).
        
        **Example**
        
            # Fetch the nodes of kind "Person" that are  
            # connected by edges of kind "Author" to other 
            # nodes of kind "Document" with tiles containing "Quantum"
            # and also collect the author order
            g.fetch('(n:Document) <(e:Author)- [p:Person,aorder]', n_fts='title: Quantum', aorder='e.data.order')
        '''
        
        ## extract the SQL pieces with sensible defaults
        WHERE=ensurelist(WHERE)
        ORDER=args.get('ORDER', None)
        GROUP=args.get('GROUP', None)
        LIMIT=args.get('LIMIT', None)
        OFFSET=args.get('OFFSET', None)
        COUNT=args.get('COUNT', False)
        DISTINCT=args.get('DISTINCT', True)    
        DEBUG=args.get('DEBUG', False)

        ## everything else is a parameter of some sort
        PARAM = {k:v for k,v in args.items() if k not in FETCHKEYWORDS}
                    
        ## interpret table joins
        aliases, collect = self._parsechain(CHAIN, PARAM)
            
        SQL = []

        SQLFTS = []
        ## SQL to attach FTS tables ... need to do this fist so we can expand fts aliases with tablename
        ftsexpansions = {}
        for k in aliases.keys():
            ftskey = k+'_fts'
            if ftskey in list(PARAM.keys()):
                ## N.B. want a copy of PARAM.keys() as we might modify PARAM
                item = aliases[k]
                SQLFTS.append('\nJOIN {ftstable} "{ftskey}" ON {alias}.uid = {ftskey}.uid'.format(
                    ftstable=item['ftstable'], ftskey=ftskey, alias=k))
                ## add an item to PARAM with the FTS term so it's SQL escaped
                valuekey = ftskey+'_value'
                ## N.B. proper reference using alias has to have table name, e.g. n1_fts.nodefts
                WHERE.append('{ftskey}.{ftstable} MATCH :{ftsvalue}'.format(
                    ftskey=ftskey, ftstable=aliases[k]['ftstable'], ftsvalue=valuekey))
                PARAM[valuekey] = PARAM[ftskey]
                del PARAM[ftskey]
                ftsexpansions[ftskey] = "{}.{}".format(ftskey,aliases[k]['ftstable'])
            
        def expandfts(ftsstring, ftsexpansions):
            for ftskey, ftsexpanded in ftsexpansions.items():
                ftsstring = ftsstring.replace(ftskey, ftsexpanded)
            return ftsstring
            
        ##
        ## SELECT
        ##
        collect['distinct'] = 'DISTINCT' if DISTINCT else ''
        colkeys = collect['columns'].copy()
        colsql = ['{}.{}'.format(collect['alias'],c) for c in colkeys]
        for k,v in collect.get('extra',{}).items():
            colkeys.append(k)
            v = jsonextract(v)
            v = expandfts(v, ftsexpansions)
            colsql.append(v)

        collect['collectcolumns'] = ', '.join(colsql)
        if COUNT:
            SQL.append('SELECT COUNT({distinct} {alias}.uid) FROM {table} {alias}'.format(**collect))
        else:
            SQL.append('SELECT {distinct} {collectcolumns} FROM {table} {alias}'.format(**collect))
    
        
        ##
        ## JOINs
        ##
        ## link tables together
        l = collect        
        while 'rightlink' in l:
            r = aliases[l['rightlink']]
            r['join'] = '{}.{} = {}.{}'.format(r['alias'], r['leftuid'], l['alias'], l['rightuid'])
            if 'kind' in r:
                r['join'] += ' AND {}.kind = "{}"'.format(r['alias'],r['kind'])
            SQL.append('\nJOIN {table} {alias} ON {join}'.format(**r))
            l=r
        r = collect
        while 'leftlink' in r:
            l = aliases[r['leftlink']]
            l['join'] = '{}.{} = {}.{}'.format(l['alias'], l['rightuid'], r['alias'], r['leftuid'] )  
            if 'kind' in l:
                l['join'] += ' AND {alias}.kind = "{kind}"'.format(**l)
            SQL.append('\nJOIN {table} {alias} ON {join}'.format(**l))
            r=l
    
        SQL.extend(SQLFTS)
        
        ##
        ## WHERE
        ##
        if 'kind' in collect:
            WHERE.append('{alias}.kind = "{kind}"'.format(**collect))

        if len(WHERE)>0:
            SQL.append('\nWHERE '+ ' AND '.join([jsonextract(w) for w in WHERE]))
        

        ##
        ## GROUP BY
        ##
        if GROUP is not None:
            SQL.append('\nGROUP BY {}'.format(expandfts(jsonextract(GROUP), ftsexpansions)))
        
        ##
        ## ORDER BY
        ##
        if ORDER is not None:
            SQL.append('\nORDER BY {}'.format(expandfts(jsonextract(ORDER),ftsexpansions)))
        
            
        ##
        ## LIMIT and OFFSET
        ##
        if LIMIT is not None:
            SQL.append('\nLIMIT {}'.format(LIMIT))
        if OFFSET is not None:
            SQL.append(' OFFSET {}'.format(OFFSET))
    
        SQL = ''.join(SQL)
        ##
        ## Return sql statement if debug
        ##
        if DEBUG:
            return SQL, PARAM
        
        cursor=self.cursor()
            
        ## faster to first create list
        items = []
        
        ##
        ## COUNT
        ##
        if COUNT:
            c = cursor.execute(SQL, PARAM).fetchone()[0]
            return c
        
        ##
        ## COLLECT
        ##        
        elif collect['type']=='node':
            for row in cursor.execute(SQL, PARAM):
                args = json.loads(row[colkeys.index('data')])
                for c,v in zip(colkeys, row):
                    if c == 'data':
                        continue                        
                    else:
                        args['_'+c] = v
                N = Node(args, graph=self, changed=False)
                items.append(N)
            return NSet(items)
        
        else:
            for row in cursor.execute(SQL, PARAM):
                args = json.loads(row[colkeys.index('data')])
                for c,v in zip(colkeys, row):
                    if c == 'data':
                        continue                        
                    else:
                        args['_'+c] = v
                E = Edge(args, graph=self, changed=False)
                items.append(E)
            return ESet(items)

    def exists(self, uid):
        '''
        Return if item exists in the database as a node or edge. UIDs are big and bad enough that they should be
        unique across all intances of nodes and edges.
        '''
        cursor = self.cursor()
        n = cursor.execute('SELECT COUNT(*) FROM nodes WHERE uid = ?',[uid]).fetchone()[0]
        if n==1:
            return True
        else:
            n = cursor.execute('SELECT COUNT(*) FROM edges WHERE uid = ?',[uid]).fetchone()[0]
            if n==1:
                return True      
            else:
                return False 

    def getuid(self, uid):
        '''
        Convenience function to find either a node or edge with a given uid.
        '''
        
        obj = self.fetch(CHAIN='(n)', WHERE='n.uid = :uid', uid=uid).one
        if obj is None:
            obj = self.fetch(CHAIN='-(e)>', WHERE='e.uid = :uid', uid=uid).one
            
        return obj
    
    def Node(self, kind=None, **args):
        '''
        Convenience method to create a new `graphydb.Node` and linked to the database.
        '''
        args['kind'] = kind
        return Node(args, graph=self)
    
    def Edge(self, startuid=None, kind=None, enduid=None, **args):
        '''
        Convenience method to create a new `graphydb.Edge` linked to the database.
        '''
        
        if isinstance(startuid, Node):
            startuid = startuid['uid']
        if isinstance(enduid, Node):
            enduid = enduid['uid']  
        args.update({'kind':kind,'startuid':startuid, 'enduid':enduid })
            
        return Edge(args, graph=self)
    
#-------------------------------------------------------------------------------- 
class GraphyDBItem(MutableMapping):
    '''
    Parent of `graphydb.Node` and `graphydb.Edge` with some common methods. Essentially acts as souped up `dict`.
    '''
    
    ## set in derived classes
    _table = ''
    _ftstable = ''    
    
    def __init__(self, data, graph=None, changed=True):
        '''
        GraphyDBItem shoudn't be instantiated directly. Use `graphydb.Node` or `graphydb.Edge` instead.
        '''
        
        self.graph = graph
        '''
        An instance of the `graphydb.Graph` holding the item.
        '''                
        
        if 'uid' not in data:
            data['uid'] = generateUUID()   
        if 'ctime' not in data:
            data['ctime'] = time.time()
        if 'mtime' not in data:
            data['mtime'] = time.time()         
        
        self.data = data
        '''Straight python dictionary that holds all the data. Keys begining with an underscore ("_")
        will be ignored when saving and can be used to store local temporary data.
        Modifying the data directly is not recommended as what's changed will not be recorded.'''    
        
        self.setChanged(changed)      
      
    def setGraph(self, graph, changed=True):
        '''
        Set the graph for the item.
        '''
        self.graph = graph
        
        self.setChanged(changed)      
        return self
      
    def __uid__(self):
        return self.data['uid']
    
    def setChanged(self, changed):
        '''
        Mark all keys as having changed.
        '''
        if changed:
            ## regard all keys as having changed
            self._changedkeys = set(self.keys())
        else:
            self._changedkeys = set()   
            
    @property   
    def changed(self):
        '''
        Returns True is any key is marked as changed.
        '''
        return len(self._changedkeys)>0
        
    @property
    def exists(self):
        '''
        Property: return True if item exists in the database otherwise False.
        '''
        cursor = self.graph.cursor()
        n = cursor.execute('SELECT COUNT(*) FROM {} WHERE uid = ?'.format(self._table), (self['uid'],)).fetchone()[0]
        if n==1:
            return True
        else:
            return False    

    def original(self):
        '''
        Return item fresh from database. 
        '''
        item = self.graph.getuid(self['uid'])
        return item
    
    def renew(self):
        '''
        Load data from database again. 
        Any local changes are discarded without setting a change item.
        Keys starting with an underscore are undisturbed.
        '''
        original = self.original()
        ## copy accross the undescore keys
        for k,v in self.data.items():
            if k[0]=='_':
                original.data[k] = v
                
        ## copy across the refreshed dataset
        self.data = original.data
        self.setChanged(False)
        return self
        
    def updatefts(self, **data):
        '''
        Update FTS for the item.
        '''
        if len(data)>0:
            cursor = self.graph.cursor()
            
            ## filter on existing column names
            columnames = [x[1] for x in cursor.execute('PRAGMA table_info({})'.format(self._ftstable)).fetchall()]
            keys = []
            values = []
            for k,v in data.items():
                if k in columnames:
                    keys.append(k)
                    values.append(v)
            if len(keys)==0:
                return
            
            n = cursor.execute("SELECT COUNT(*) FROM {} WHERE uid = ?".format(self._ftstable), [self['uid']]).fetchone()[0]
            if n > 0:
                ## use UPDATE
                keystr = ",".join( ["{} = ?".format(k) for k in keys] )
                query = 'UPDATE {} SET {} WHERE uid = "{}"'.format(self._ftstable, keystr, self['uid'])
            else:
                ## use INSERT
                keystr = ",".join(keys)+",uid"
                values.append(self['uid'])
                qstr = ",".join(['?']*len(values))
                query = 'INSERT INTO {}({}) VALUES ({})'.format(self._ftstable, keystr, qstr)
                
            cursor.execute(query, values)
        return self

            
    def set(self, **attr):
        '''
        Set a bunch of keys in one go.
        '''
        for k,v in attr.items():
            self[k] = v
            self._changedkeys.add(k)
        return self

    def __getitem__(self, key):
        if key in self.data:
            return self.data[key]
        if hasattr(self.__class__, "__missing__"):
            return self.__class__.__missing__(self, key)
        raise KeyError(key)
    
    def __setitem__(self, key, item):
        self.data.__setitem__(key, item)
        if key != 'mtime':
            # avoid recursion!
            self['mtime'] = time.time()
        self._changedkeys.add(key)
        
    def __delitem__(self, key):
        self.data.__delitem__(key)
        if key != 'mtime':
            # avoid recursion!
            self['mtime'] = time.time()
        self._changedkeys.add(key)
    
    def discard(self, key):
        '''
        Remove key if present
        '''
        if key in self.data:
            del self[key]
        return self
    
    def deletefts(self):
        '''
        Remove the FTS data for this item.
        '''
        cursor = self.graph.cursor()
        if cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{}';".format(self._ftstable)).fetchone()[0] >0:
            cursor.execute('DELETE FROM {} WHERE uid = ?'.format(self._ftstable), (self['uid'],))
        return self
            
    def __len__(self): 
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __contains__(self, key):
        return key in self.data

    ## Now, add the methods in dicts but not in MutableMapping
    
    def __repr__(self): 
        return repr(self.data)
    
    def copy(self, newuid=False):
        data = self.data
        try:
            self.data = {}
            c = copy.copy(self)
        finally:
            self.data = data
        c.update(self)
        c._changedkeys = set(self._changedkeys)
        if newuid:
            c['uid'] = generateUUID()
        return c
    
    def deepcopy(self, newuid=False):
        data = self.data
        try:
            self.data = {}
            c = copy.deepcopy(self)
        finally:
            self.data = data
        c.data = copy.deepcopy(data)
        if newuid:
            c['uid'] = generateUUID()
        return c        
    
    @classmethod
    def fromkeys(cls, iterable, value=None):
        d = cls()
        for key in iterable:
            d[key] = value
            self._changedkeys.add(key)
        return d


#-------------------------------------------------------------------------------- 
class Node(GraphyDBItem):
    '''
    A Node object
    can contain abitrary key value pairs as long as they are JSONifiable 
    '''
    
    _table = 'nodes'
    _ftstable = 'nodefts'
    
    def __init__(self, data, graph=None, changed=True):
        if data.setdefault('kind', None) is None:
            raise GraphyDBException("Must supply Node kind")        
        super().__init__(data, graph=graph, changed=changed)
        
    def save(self, force=False, batch=None, setchange=True):
        '''
        Save the data to the database. Any keys that begin with "_" will *not* be saved.
        
        - `force`: if `True` will save regardless if item marked as changed.
        '''
        ## ignore if unforced and not changed
        if not force and not self.changed:
            return self        
        
        cursor = self.graph.cursor()
        data = cleandata(self.data)
                
        if setchange:
            originalitem = self.original()
        
        cursor.execute("INSERT OR REPLACE INTO nodes(uid, kind, ctime, mtime, data) VALUES(?,?,?,?,?)", 
                       (self['uid'], self['kind'], self['ctime'], self['mtime'], json.dumps(data)) )
        
        if setchange:
            self.graph.addchange(old=originalitem, new=self, batch=batch)                        
        
        self.setChanged(False)
        return self
        
        
    def inE(self, WHERE=None, **args):
        '''
        Fetch incomming edges i.e. "<[e]-" with "e.enduid = self.uid" 
        (see `Graph.fetch` for details)
        '''
        args['CHAIN'] = '<(e)-'
        args['WHERE'] = ensurelist(WHERE)
        args['WHERE'].insert(0,'e.enduid = :node_uid')
        args['node_uid'] = self['uid']
            
        return self.graph.fetch(**args)
        
    def outE(self, WHERE=None, **args):
        '''
        Fetch outgoing edges, i.e. "-[e]>" with "e.startuid = self.uid" 
        (see `Graph.fetch` for details)
        '''
        args['CHAIN'] = '-(e)>'
        args['WHERE'] = ensurelist(WHERE)
        args['WHERE'].insert(0,'e.startuid = :node_uid')
        args['node_uid'] = self['uid']
            
        return self.graph.fetch(**args)   
    
    def bothE(self, WHERE=None, **args):
        '''
        Get edges both incomming and outgoing 
        (see `Graph.fetch` for details)
        '''
        args['WHERE'] = ensurelist(WHERE)
        ## A deep copy is necessary as inE and outE 
        ## modify the dict or lists withing the dict
        if args.get('COUNT', False):
            ## COUNT=True will fail as it doesn't check uniqueness across   
            ## in and out sets fetch actual items and count in python
            args['COUNT'] = False
            ine = self.inE(**copy.deepcopy(args))
            oute = self.outE(**copy.deepcopy(args))            
            return len(ine|oute)
        else:
            ine = self.inE(**copy.deepcopy(args))
            oute = self.outE(**copy.deepcopy(args))            
            ## union of sets
            return ine | oute 
    
    def inN(self, WHERE=None, **args):
        '''
        Fetch nodes on an incomming edge i.e. "<(e)- [n]" with "e.enduid = self.uid" 
        (see `Graph.fetch` for details)
        '''
        args['CHAIN'] = '<(e)- [n]'
        args['WHERE'] = ensurelist(WHERE)     
        args['WHERE'].insert(0,'e.enduid = :node_uid')
        args['node_uid'] = self['uid']
            
        return self.graph.fetch(**args) 
    
    def outN(self, WHERE=None, **args):
        '''
        Fetch nodes on an outgoing edge "-(e)> [n]" with "e.startuid = self.uid" 
        (see `Graph.fetch` for details)
        '''
        args['CHAIN'] = '-(e)> [n]'
        args['WHERE'] = ensurelist(WHERE)        
        args['WHERE'].insert(0,'e.startuid = :node_uid')
        args['node_uid'] = self['uid']
            
        return self.graph.fetch(**args) 
    
    def bothN(self, WHERE=None, **args):
        '''
        Fetch nodes connected by edge 
        (see `Graph.fetch` for details)
        '''
        args['WHERE'] = ensurelist(WHERE)
        ## A deep copy is necessary as inE and outE 
        ## modify the dict or lists withing the dict 
        if args.get('COUNT', False):
            ## COUNT=True will fail as it doesn't check uniqueness across    
            ## in and out sets fetch actual items and count in python
            args['COUNT'] = False
            inn = self.inN(**copy.deepcopy(args))
            outn = self.outN(**copy.deepcopy(args))              
            return len(inn|outn)
        else:
            inn = self.inN(**copy.deepcopy(args))
            outn = self.outN(**copy.deepcopy(args))            
            ## union of sets
            return inn | outn         
            
    def delete(self, disconnect=False, batch=None, setchange=True):
        '''
        Delete this node from the database.
        
        `disconnect`: If `True`, silently delete any connected edges, else raise an Exception
        if the node is connected and deleting it would leave the graph inconsistent.
        '''
        cursor = self.graph.cursor()
        if self.outE(COUNT=True)+self.inE(COUNT=True) > 0:
            if disconnect:
                if setchange and batch is None:
                    ## if no batch set, set one now to group all the edges and node in a single change set
                    batch = generateUUID()                 
                for edge in self.bothE():                
                    edge.delete(batch=batch, setchange=setchange)                           
            else:
                raise GraphyDBException("Node still connected. Delete Edges First")
        
        cursor.execute('DELETE FROM nodes WHERE uid = ?', (self['uid'],))
        
        if setchange:  
            self.graph.addchange(old=self, batch=batch)
        
        
        self.deletefts()
        self['mtime'] = time.time()
        self.setChanged(True)
        return self
        
    def __repr__(self):
        return '({uid}:{kind})'.format(**self.data)
   
#-------------------------------------------------------------------------------- 
class Edge(GraphyDBItem):
    '''
    A Edge object
    can contain abitrary key value pairs as long as they are JSONifiable 
    '''
    
    _table = 'edges'
    _ftstable = 'edgefts'
    
    def __init__(self, data, graph=None, changed=True):
        
        if data.setdefault('kind', None) is None:
            raise GraphyDBException("Must supply edge kind")      
            
        if data.setdefault('startuid', None) is None:
            raise GraphyDBException("Wrong type or missing start node")

        if data.setdefault('enduid', None) is None:
            raise GraphyDBException("Wrong type or missing end node")

        super().__init__(data, graph=graph, changed=changed)
        
    def save(self, force=False, batch=None, setchange=True):
        '''
        Save the data to the database. Any keys that begin with "_" will *not* be saved.
        
        - `force`: if `True` will save regardless if item marked as changed.
        '''
        
        ## ignore if unforced and not dirty
        if not force and not self.changed:
            return self
        
        if not self.graph.exists(self['startuid']): 
            raise GraphyDBException('start node referenced from edge does not exist in DB.')
        if not self.graph.exists(self['enduid']):
            raise GraphyDBException('end node referenced from edge does not exist in DB.')
        
        data = cleandata(self.data)   
                
        if setchange:
            originalitem = self.original()                

        cursor = self.graph.cursor()
        cursor.execute("INSERT OR REPLACE INTO edges(uid, startuid, kind, enduid, ctime, mtime, data) VALUES(?,?,?,?,?,?,?)", 
                       (self['uid'], self['startuid'], self['kind'], self['enduid'], self['ctime'], self['mtime'], json.dumps(data)) )
        
        if setchange:
            self.graph.addchange(old=originalitem, new=self, batch=batch)                        
        
        self.setChanged(False)
        return self

            
    def delete(self, setchange=True, batch=None):
        '''
        Delete edge from database.
        '''
        cursor = self.graph.cursor()
        cursor.execute('DELETE FROM edges WHERE uid = ?', (self['uid'],))
        self.deletefts()
        self['mtime'] = time.time()
        
        if setchange:
            self.graph.addchange(old=self, batch=batch)
        
        self.setChanged(True)
        return self
            
    @property
    def start(self):
        '''
        Return node at start of directed edge
        '''
        return self.graph.fetch(CHAIN='(n)', WHERE='n.uid = :start_uid', start_uid=self['startuid']).pop()
    
    @property
    def end(self):
        '''
        Return node at end of directed edge
        '''
        return self.graph.fetch(CHAIN='(n)', WHERE='n.uid = :end_uid', end_uid=self['enduid']).pop()
    
    def __repr__(self):
        return '({startuid})-[{uid}:{kind}]->({enduid})'.format(**self.data)
       

#--------------------------------------------------------------------------------    
class GraphyDBItemSet(IndexedSet):
    '''
    Super class of sets `graphydb.NSet` and `graphydb.ESet` holding nodes and edges.
    Operations between sets will be based entirely on the items  `__uid__()` not on their content.
    Methods will return a reference to itself where appropriate to allow chaining of commands.
    '''
    
    def setGraph(self, graph, changed=True):
        '''
        Set the graph on all contained items. Items not saved to new graph automatically.
        '''
        for item in self:
            item.setGraph(graph, changed)
        return self
    
    def save(self, force=False, batch=None, setchange=True):
        '''
        Save all items to the database.
        
        - `force`: if `True`, save regardless if the item has changed.
        '''
        if batch is None:
            ## since we're saving in a group this should be batched
            batch = generateUUID()        

        for item in self:
            item.save(force=force, batch=batch, setchange=setchange)
        return self
            
    
    def filter(self, function):
        '''
        Pythonic filter method on the set. Returns a set with items where the function
        returns `True`. Returned items are referenced not copies.
        
            fruits = ['Orange','Apple','Pear']
            barset = fooset.filter(lambda n: n['fruit'] in fruits])
        '''
        ## ensure we have the same type of set: either NSet or Eset
        out = self.__class__()
        
        ## this way is about twice as slow as using filter
        ## but we can make it insensitive to missing keys etc
        for item in self:
            try:
                if function(item):
                    out.add(item)
            except:
                pass
        return out
    
    def filter_fnmatch(self, **attr):
        '''
        Apply `fnmatch` to all the keys given and return the set of items that match. 
        Returned items are referenced not copies.
        
            barset = fooset.filter_fnmatch(title='Once Upon *')
        '''
        
        out = self.__class__()
        
        for item in self:
            found = True
            for key, pattern in attr.items():
                try:
                    found = found and fnmatch.fnmatch(item[key], pattern)
                except KeyError:
                    found = False
                    break  
            if found:
                out.add(item)
                
        return out
    
    @property
    def one(self):
        '''
        Return a single item from set or `None` if empty. Set not modified.
        '''
        if len(self)==0:
            return None
        else:
            return self[0]

    def get(self, key, default=None):
        '''
        Get the values of the key for each item in the set as a list. 
        Return the `default` for each item without that key.
        '''
        out = []
        for item in self:
            out.append(item.get(key, default))
        return out

    def getm(self, *keys, default=None):
        '''
        Get a list of values of the keys for each item in the set as a list. 
        Return the `default` for each item without a key.
        '''
        out = []
        for item in self:
            out.append([item.get(key, default) for key in keys])
        return out  
  
    def set(self, **attr):
        '''
        Set a bunch of attributes in one go on each item in the set.
        '''
        for item in self:
            item.set(**attr)
        return self
    
    def deletefts(self):
        '''
        Remove the FTS data from the database for the items in the set.
        '''
        for item in self:
            item.deletefts()
        return self
            
    
#--------------------------------------------------------------------------------    
class ESet(GraphyDBItemSet):
    '''
    A set holding edges with some agregate functionality.
    '''
    
    @property
    def end(self):
        '''
        The nodes at the ends of the edges in the set. Fetched from the database.
        '''
        out = NSet()
        for e in self:
            out.add(e.end)
        return out
    
    @property
    def start(self):
        '''
        The nodes at the start of the edges in the set. Fetched from the database.
        '''
        out = NSet()
        for e in self:
            out.add(e.start)
        return out

    def delete(self, batch=None, setchange=True):
        '''
        Delete the items from the *database*.
        N.B. don't confuse with remove() and discard() which work only on the set!
        '''
        if setchange and batch is None:
            ## since we're deleting in a group this should be batched
            batch = generateUUID()
            
        for item in self:
            item.delete(batch=batch, setchange=setchange)
            
#--------------------------------------------------------------------------------    
class NSet(GraphyDBItemSet):
    '''
    A set holding nodes with some agregate functionality.
    '''    
    
    def inE(self, WHERE=None, **args):
        '''
        Fetch incoming edges to all the nodes in the set.
        '''
        out = ESet()
        args['WHERE'] = ensurelist(WHERE)
        for v in self:
            out.update(v.inE(**copy.deepcopy(args)))
        return out
    
    def outE(self, WHERE=None, **args):
        '''
        Fetch outgoing edges to all the nodes in the set.
        '''
        out = ESet()
        args['WHERE'] = ensurelist(WHERE)
        for v in self:
            out.update(v.outE(**copy.deepcopy(args)))
        return out
    
    def bothE(self, WHERE=None, **args):
        '''
        Fetch both incoming and outgoing edges to all the nodes in the set.
        '''
        out = ESet()
        args['WHERE'] = ensurelist(WHERE)
        for v in self:
            out.update(v.bothE(**copy.deepcopy(args)))
        return out
    
    def inN(self, WHERE=None, **args):
        '''
        Fetch nodes on an incomming edge to the nodes in the set. 
        This may include nodes in the set itself.
        '''
        out = NSet()
        args['WHERE'] = ensurelist(WHERE)
        for v in self:
            out.update(v.inN(**copy.deepcopy(args)))
        return out
    
    def outN(self, WHERE=None, **args):
        '''
        Fetch nodes on outgoing edges to the nodes in the set.
        This may include nodes in the set itself.
        '''
        out = NSet()
        args['WHERE'] = ensurelist(WHERE)
        for v in self:
            out.update(v.outN(**copy.deepcopy(args)))
        return out
    
    def bothN(self, WHERE=None, **args):
        '''
        Fetch nodes attached to the nodes in the set.
        This may include nodes in the set itself.
        '''
        out = NSet()
        args['WHERE'] = ensurelist(WHERE)
        for v in self:
            out.update(v.bothN(**copy.deepcopy(args)))
        return out    
    
    def delete(self, disconnect=False, batch=None, setchange=True):
        '''
        Delete the items from the *database*.
        N.B. don't confuse with remove() and discard() which work only on the set!
        '''
        if setchange and batch is None:
            ## since we're deleting in a group this should be batched
            batch = generateUUID()
            
        for item in self:
            item.delete(disconnect=disconnect, batch=batch, setchange=setchange)
            
def _debug():
    ## Used to help debug 
    try:
        import wingdbstub
    except:
        pass    

# ===============================================================================
if __name__ == "__main__":

    ## for debugging ...
    logging.info("Program started on %s", datetime.now().isoformat())
