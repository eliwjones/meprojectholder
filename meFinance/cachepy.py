"""
Author: Juan Pablo Guereca

Module which implements a per GAE instance data cache, similar to what you can achieve with APC in PHP instances.

Each GAE instance caches the global scope, keeping the state of every variable on the global scope. 
You can go farther and cache other things, creating a caching layer for each GAE instance, and it's really fast because
there is no network transfer like in memcache. Moreover GAE doesn't charge for using it and it can save you many memcache
and db requests. 

Not everything are upsides. You can not use it on every case because: 

- There's no way to know if you have set or deleted a key in all the GAE instances that your app is using. Everything you do
  with Cachepy happens in the instance of the current request and you have N instances, be aware of that.
- The only way to be sure you have flushed all the GAE instances caches is doing a code upload, no code change required. 
- The memory available depends on each GAE instance and your app. I've been able to set a 60 millions characters string which
  is like 57 MB at least. You can cache somethings but not everything. 
"""

import time
import logging
import os
import medict

CACHE = medict.SizedDict(2000000)
PRIORITY_CACHE = medict.SizedDict(3000)
CACHES = [CACHE,PRIORITY_CACHE]

ACTIVE = True
DEFAULT_CACHING_TIME = None
URL_KEY = 'URL_%s'

"""
Curious thing: A dictionary in the global scope can be referenced and changed inside a function without using the global statement,
but it can not be redefined.
"""

def get( key, priority=0 ):
    if ACTIVE is False:
        return None
        
    global CACHES, PRIORITY_CACHE, CACHE
        
    """ Return a key stored in the python instance cache or a None if it has expired or it doesn't exist """
    if key not in CACHES[priority]:
        return None
    
    value,expiry = CACHES[priority][key]
    current_timestamp = time.time()
    if expiry is None or current_timestamp < expiry:
        return value
    else:
        delete( key )
        return None

def get_multi(keylist, priority=0):
    multilist = {}
    
    if ACTIVE is False:
        return None

    global CACHES, PRIORITY_CACHE, CACHE

    for key in keylist:
        if key in CACHES[priority]:
            value, expiry = CACHES[priority][key]
            current_timestamp = time.time()
            if expiry is None or current_timestamp < expiry:
                multilist[key] = value
    return multilist
    

def set( key, value, priority=0, expiry = DEFAULT_CACHING_TIME ):
    if ACTIVE is False:
        return None
    
    global CACHES, PRIORITY_CACHE, CACHE

    if expiry != None:
        expiry = time.time() + int( expiry )
    try:
        CACHES[priority][key] = ( value, expiry )
    except MemoryError:
        """ It doesn't seems to catch the exception, something in the GAE's python runtime probably """
        logging.info( "%s memory error setting key '%s'" % ( __name__, key ) )
 
def delete( key, priority=0 ):
    """ 
    Deletes the key stored in the cache of the current instance, not all the instances.
    There's no reason to use it except for debugging when developing, use expiry when setting a value instead.
    """
    global CACHES, PRIORITY_CACHE, CACHE
    if key in CACHES[priority]:
        del CACHES[priority][key]
