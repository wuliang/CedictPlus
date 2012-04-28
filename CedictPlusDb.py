# -*- coding: utf8 -*-
import collections
import logging
import math
import os
import random
import re
import sqlite3
import time
import types

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def dict_show(myDict):
    print '-' * 80
    for key in myDict.keys():
        print key, ' : ',  myDict[key]    
 
class CedictPlusDb:
    """Database functions to support a Cobe brain. This is not meant
    to be used from outside."""
    def __init__(self, filename, rebuild=False):
        if rebuild:
            os.unlink(filename)
        if not os.path.exists(filename):
            try:
                self.init(filename)
            except:
                os.unlink(filename)
                raise
                
        self.conn = sqlite3.connect(filename)
        self.conn.row_factory = dict_factory        
        self.conn.text_factory = str
        self.insert_errnum = 0
        self.update_errnum = 0
        self.remove_errnum = 0
        self.execute_num = 0
        self.commit_period = 2000
        
    def commit(self):
        ret = self.conn.commit()
        return ret
        
    def close(self):
        
        self.conn.cursor().close()
        self.conn.close()

    # myDict is used for reference 
    def remove_dict(self,  table,  id, myDict):
        c = self.conn.cursor()
        try:
            qry = "DELETE FROM %s WHERE id=%s" % (table, id)
            c.execute(qry, ())               
            self.commit()
        except sqlite3.IntegrityError:
            #print "in table %s, a update dict error occur" % table 
            #dict_show(myDict)
            self.remove_errnum = self.remove_errnum + 1
       
    def search_dict(self, word):
        c = self.conn.cursor()
        q = "select * from dict where simplified = ?"
        rows = c.execute(q, (word, )).fetchall()
        if rows:
            return rows[0]
        return None
        
    def search_dict_with_path(self, word):
        c = self.conn.cursor()
        q = "select * from dict where simplified = ?"
        rows = c.execute(q, (word, )).fetchall()
        if not rows:
            return []
        treeid = rows[0]['treeid']
        return self.fetch_subtree("/" + str(treeid))

    def fetch_dict_all(self,  n=None):
        c = self.conn.cursor()        
        if n is None:
            q = "select * from dict"
        else:
            q = "select * from dict limit %d" % n
        rows = c.execute(q, ()).fetchall()        
        return rows
        
    def fetch_subtree(self, xpath):
        c = self.conn.cursor()
        if xpath == '/':
            xpath = ""
        q = "select * from dict_tree where path=? or path like ? || '/%' order by  path || '/' || cast(id as varchar)"
        rows = c.execute(q, (xpath,  xpath)).fetchall()
        return rows
        
    def delete_subtree(self, path):
        c = self.conn.cursor()
        q = "delete from dict_tree where path like ? || '/%' "
        c.execute(q, (path, ))
        self.commit()        
        return 
        
    def move_subtree(self, spath,  dpath):
        c = self.conn.cursor()
        plen = len(spath)
        q = "update dict_tree set path=? || substr(path, ?) where path like ? || '/%' "
        c.execute(q, (dpath, plen,  spath ))
        self.commit()
        return
          
        
    def insert_dict(self,  table,  myDict):
        c = self.conn.cursor()
        
        qmarks = ', '.join('?' * len(myDict))
        kmarks = ', '.join(myDict.keys())
        try:
            qry = "Insert Into %s (%s) Values (%s)" % (table, kmarks, qmarks)
            c.execute(qry, myDict.values())   
            self.execute_num += 1
            if self.execute_num % self.commit_period == 0:
                self.commit()
        except sqlite3.IntegrityError:
            self.insert_errnum = self.insert_errnum + 1
                
    def insert_dict_entry(self,  dict):
        self.insert_dict("dict",  dict)
        return        

    def insert_tree_entry(self,  dict_tree):
        self.insert_dict("dict_tree",  dict_tree)     
        return
         
        
    def init(self, filename):
        self.conn = sqlite3.connect(filename)
        c = self.conn.cursor()           
        c.execute("""
            create table dict (
                traditional VARCHAR(255), 
                simplified VARCHAR(255), 
                reading   VARCHAR(255), 
                pos      VARCHAR(255),
                translation VARCHAR(255), 
                treeid int          
            )  """    )
        
        c.execute("""
            create table dict_tree (
                id int IDENTITY(1,1) PRIMARY KEY,
                path VARCHAR(200) not null,
                raw VARCHAR(255), 
                pos VARCHAR(255)      
            ) """    )
        
        c.execute("""
        CREATE INDEX dict__simplified ON dict (simplified)
        """)         
        c.execute("""
        CREATE INDEX dict__traditional ON dict (traditional)
        """)    
        c.execute("""
        CREATE INDEX dict_tree__id ON dict_tree (id)
        """) 
        c.execute("""
        CREATE INDEX dict_tree__path ON dict_tree (path)
        """) 
               
        self.commit()
        self.close()

def main():

#    db = CedictPlusDb("cedictPlus.db")
#    tree = {}
#    tree["id"] = 1    
#    tree["path"] = "/"
#    tree["raw"] = "AAA"
#    db.insert_tree_entry(tree)
#    
#    tree["id"] = 2  
#    tree["path"] = "/1"
#    tree["raw"] = "2: /1"
#    db.insert_tree_entry(tree)
#
#    tree["id"] = 3 
#    tree["path"] = "/1"
#    tree["raw"] = "3: /1"
#    db.insert_tree_entry(tree)
#
#    tree["id"] = 4 
#    tree["path"] = "/1/2"
#    tree["raw"] = "4: /1/2"
#    db.insert_tree_entry(tree)

    word = u"一会"
    db = CedictPlusDb("cedict-plus.db")        
    entry = db.search_dict(word)
    dict_show(entry)
    rows = db.search_dict_with_path(word)
    for row in rows:
        print row['path'] + "/" + str(row['id']),  row['raw']
        
if __name__ == "__main__":
    main()
