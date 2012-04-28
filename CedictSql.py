# -*- coding: utf8 -*-
import sys
import exceptions
import os
import sqlite3
import codecs
import CedictPlusDb


def dict_show(myDict):
    print '-' * 80
    for key in myDict.keys():
        print key, ' : ',  myDict[key]    
 
def splitSentence(text, delimiters=None):

    if delimiters is None:
        delimiters = "\n\r"

    sentence = []
    for c in text:
        if c in delimiters:
            yield ''.join(sentence)
            sentence = []
        else:
            sentence.append(c)
    yield ''.join(sentence)
    
class _Sql():
    def __init__(self, filename):
        if not os.path.exists(filename):
            raise exceptions.Exception("Can't find the filename %s" % filename)

        self.conn = sqlite3.connect(filename)
        self.conn.row_factory = self.dict_factory
        self.conn.text_factory = str
        
    @staticmethod
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    @staticmethod
    def dict_show(myDict):
        print '-' * 80
        for key in myDict.keys():
            print key, ' : ',  myDict[key]  
        
    def commit(self):
        ret = self.conn.commit()
        return ret
        
    def close(self):
        
        self.conn.cursor().close()
        self.conn.close()
   

    def fetch_chinese_all(self, n=None):

        c = self.conn.cursor()
        if n is None:
            q = "SELECT * FROM CEDICT"
        else:
            q = "SELECT * FROM CEDICT LIMIT %d" % n
        rows = c.execute(q, ()).fetchall()
        return rows
      
class _Parser():
    def __init__(self):
        self.id = 0
        
    def node_list(self, root):
        list = []
        for cl in root['C'] :
            list.append(cl)
            list.extend(self.node_list(cl))
        return list
            
        
    def show_parse_tree(self,  root,  text, dict):
        showtxt = ""
        def _dump_node(node, text):
            if node['S'] >= node['E']:
                return "Error\n"
            node['ID'] = self.id
            self.id =  self.id + 1
            node_txt = ("[%d]" % node['L']) +"\t" * node['L'] + text[node['S']:node['E']].strip() 
            if dict is not None:
                pos_info = " POS: "
                if node['ID'] not in dict:
                    pos_info = " * no POS info * "
                else:
                    dentry = dict[node['ID']]
                    txt_list = dentry['txt']
                    pos_list = dentry['pos']
                    for n in range(len(txt_list)):
                        pos_info = pos_info + " {%s | %s}" % (txt_list[n],  pos_list[n])
                node_txt =node_txt + pos_info + "\n"
            return node_txt
        showtxt = showtxt + _dump_node(root, text)
        for child in root['C']:
            showtxt = showtxt + self.show_parse_tree(child,  text, dict)
        return showtxt
    
    def pos_only_one(self,  pos_list,  raw_txt):
        short_list = [self.pos_short_form(pos) for pos in pos_list]
        # see is a special word in dictionary. remove it
        if 'see' in raw_txt and 'verb' in short_list:
            short_list.remove('verb')
            
        if 'verb' in short_list:
            return 'verb'
        elif 'noun' in short_list:
            return 'noun'
        elif 'adj' in short_list:
            return 'adj'
        elif 'adv' in short_list:
            return 'adv'
        else:
            return 'sub'
            
    def pos_short_form(self,  pos):
#• Nouns (NN, NNS, NNP, NNPS), adverbial nouns,
#• Adjectives (JJ, JJR, JJS), Numbers (CD),
#• Verbs (VB, VBP, VBZ, VBG, VBD, VBN);
#• Adverbs (RB, RBR, RBS)
#• Remaining POS classes:
#– existential there (EX), foreign words (FW), list items (LS),
#– punctuation, symbols (SYM)
        pos = pos.strip()
        if pos in ['NN', 'NNS', 'NNP', 'NNPS']:
            spos = 'noun'
        elif pos in ['VB', 'VBP', 'VBZ', 'VBG', 'VBD', 'VBN']:
            spos = 'verb'
        elif pos in ['JJ', 'JJR', 'JJS',  'CD']:
            spos = 'adj'
        elif pos in ['RB',  'RBR',  'RBS']:
            spos = 'adv'
        else:
            spos = 'sub'            
        return spos

    def beautify_translation(self, raw_txt):
        raw_txt = raw_txt.strip()
        if len(raw_txt) >= 3:
                rlen = len(raw_txt)
                if (raw_txt[0],  raw_txt[rlen-1]) in [('[',  ']'), ('{',  '}'),  ('/',  '/'),  ('(',  ')'), ('<',  '>')]:
                    raw_txt = raw_txt[1:rlen-1]
        return raw_txt
        
    def insert_parse_2db(self,  root,  text, dict,  db, sdict):
        info = self.insert_parse_tree_2db(root,  text, dict,  db)
        dbent = {}
        dbent['traditional'] = sdict['HeadwordTraditional']
        dbent['simplified'] = sdict['HeadwordSimplified']
        dbent['reading'] = sdict['Reading']
        dbent['translation'] = self.beautify_translation(sdict['Translation'])
        dbent['pos'] = info['pos']              
        dbent['treeid'] = info['id']
        db.insert_dict_entry(dbent)  
        
    def insert_parse_tree_2db(self,  root,  text, dict,  db):
        def _insert_node_2db(node, text):
            result = {}
            if node['S'] >= node['E']:
                return result
            dbent = {}
            pos_count = {}
            node['ID'] = self.id           
            self.id =  self.id + 1
            raw_txt = text[node['S']:node['E']].strip()
            pos_txt = []
            pos_list = []
            if dict is not None:
                if node['ID'] in dict:
                    dentry = dict[node['ID']]
                    txt_list = dentry['txt']
                    pos_list = dentry['pos']
                    for n in range(len(txt_list)):
                        pos_txt.append("%s|%s" % (txt_list[n],  pos_list[n]))
                        pos_count.setdefault(pos_list[n], 0)
                        pos_count[pos_list[n]] += 1
            pos_txt = ",".join(pos_txt)
            dbent['id'] = node['ID']
            if node['L'] == 0:
                node['path'] = "/"
            elif node['L'] == 1:
                parent = node['P']                
                node['path'] = "/" + str(parent['ID'])               
            else:
                parent = node['P']
                node['path'] =  parent['path'] + "/" + str(parent['ID'])
            dbent['path'] = node['path']
            if len(raw_txt) >= 3:
                rlen = len(raw_txt)
                if (raw_txt[0],  raw_txt[rlen-1]) in [('[',  ']'), ('{',  '}'),  ('/',  '/'),  ('(',  ')'), ('<',  '>')]:
                    raw_txt = raw_txt[1:rlen-1]
            dbent['raw'] = raw_txt
            dbent['pos'] = pos_txt
            db.insert_tree_entry(dbent) 
            if node['L'] == 0:
                result['pos'] = self.pos_only_one(pos_list, raw_txt)
                result['id'] = node['ID']
            return result
            
        info = _insert_node_2db(root, text)
        for child in root['C']:
            self.insert_parse_tree_2db(child,  text, dict, db)
        return info

    def output_parse_tree(self,  root,  text):
        showtxt = ""
        def _dump_node(node, text):
            node['ID'] = self.id
            self.id =  self.id + 1            
            node_txt = text[node['S']:node['E']].strip("/") + "\n"
            return node_txt
        showtxt = showtxt + _dump_node(root, text)
        for child in root['C']:
            showtxt = showtxt + self.output_parse_tree(child,  text)
        return showtxt
        
    def parse_all(self,  items, output=None, dict=None,  db=None):
        self.id = 0      
        failed = []
        for item in items:
            if len(item) == 0:
                continue
            if 'Translation' not in item:
                continue
            transitem = item['Translation']            
            tree = self.english_entry_parse(transitem)
            if tree is None:
                failed.append(transitem)
                continue
            if output == None:
                if db == None:
                    print self.show_parse_tree(tree, transitem, dict)
                else:
                    #print self.show_parse_tree(tree, transitem, dict)
                    self.insert_parse_2db(tree, transitem, dict, db, item)
            else:
                print >>output, self.output_parse_tree(tree, transitem)
        if len(failed) > 0:
            print "Failed to parse %d sentences:" % len(failed)
            for sent in failed:
                print sent
                
    def show_node(self,  text,  name,  node):
        if node is None:
            str = name + "None"
        else:
            str = "%s[L:%d]: %s" % (name , node['L'],  text[node['S']:node['E']])
        print str
        
    def english_entry_parse(self,  text):
        '''
        '''
        length = len(text)
        sepers = []
        root = {'L':0, 'S':0, 'E':length+1, 'P':None, 'C':[], 'W':''}
        parent = None
        node = root
        for n in range(length):
            
            ch = text[n]
            if ch == node['W']:
                node['E'] = n + 1          
                if sepers:
                    subs = []                
                    prev = node['S'] + 1
                    sepers.append(n)
                    for seper in sepers:                    
                        subnode = {'L':node['L']+1, 'S':prev, 'E':seper, 'P':node, 'C':[], 'W':''} 
                        prev = seper + 1

                        for child in node['C']:                        
                            if child['S'] >= subnode['S'] and child['E'] <= subnode['E']:
                                subnode['C'].append(child)
                                child['P'] = subnode
                        for child in self.node_list(subnode):
                            child['L'] += 1
                        subs.append(subnode)
                        node['C'] = subs
                sepers= []
                node = node['P']
                parent = node['P']
                
                if ch != '/':
                    continue
            elif ch == ';': 
                # here is a bug. should recored which level it belongs
                sepers.append(n)
                    
            if ch == '/':

                if node != root:
                    print text # for debug
                    return None

                if n + 1 == length:
                    break
                newnode = {'L':node['L']+1, 'S':n, 'E':n+1, 'P':node, 'C':[], 'W':'/'}
                node['C'].append(newnode)
                parent, node = node, newnode

            elif ch == '[':

                newnode = {'L':node['L']+1, 'S':n, 'E':n+1, 'P':node,'C':[], 'W':']'}
                node['C'].append(newnode) 
                parent, node = node, newnode
                
            elif ch == '(':
              
                newnode = {'L':node['L']+1, 'S':n, 'E':n+1, 'P':node,'C':[], 'W':')'}
                node['C'].append(newnode)    
                parent, node = node, newnode
                           
                
        return root

def cedit_parse_to_txt():
    if len(sys.argv) < 4:
        print "Usage:",  sys.argv[0],  "o",  "dbname",  "filename"
        print "please check you parameters. current number is %d" % (len(sys.argv) - 1)
        return
    dbname = sys.argv[2]
    filename = sys.argv[3]

    # , encoding='utf8'
    file = codecs.open(filename, 'wt')
    if not file:
        return
        
    sql =_Sql(dbname)
    rows = sql.fetch_chinese_all(n=None)    
    par = _Parser() 
    par.parse_all(rows,  output=file, dict=None)
    
def cedit_parse_with_dict():
    if len(sys.argv) < 4:
        print "Usage:",  sys.argv[0],  "i",  "dbfile",  "dict(pos-file)"
        print "please check you parameters. current number is %d" % (len(sys.argv) - 1)
        return

    srcfile = sys.argv[2]
    dicfile = sys.argv[3]

    f1 = open(dicfile,'r')    
    if not f1:
        return
 
    print "Loading Dict..."
    lineno = 0
    myDict = {}
    for sent in f1:
        sent = sent.strip(" \r\n\t")
        if len(sent) == 0:
            continue
        parts = sent.split()
        pos_list =[]
        txt_list = []
        for part in parts:
            part = part.strip(" \r\n\t")
            vs = part.split('/')
            if len(vs) < 2:
                print part
                print vs
            assert len(vs) >= 2
            txt_list.append(vs[0])
            pos_list.append(vs[1])
        myDict[lineno] = {'txt':txt_list,  'pos':pos_list}
        lineno += 1
    print "Loaded."

    sql =_Sql(srcfile)
    plusdb = CedictPlusDb.CedictPlusDb("Plus"+srcfile,  rebuild=True)    
    rows = sql.fetch_chinese_all(n=None)    
    par = _Parser()
    par.parse_all(rows,  output=None, dict=myDict, db=plusdb)
    plusdb.commit()


def export_cedictplus_txt():
    if len(sys.argv) < 4:
        print "Usage:",  sys.argv[0],  "e",  "dbname",  "filename"
        print "please check you parameters. current number is %d" % (len(sys.argv) - 1)
        return
    dbname = sys.argv[2]
    filename = sys.argv[3]
    # encoding='utf8'
    file = codecs.open(filename, 'wt')
    if not file:
        return
    plusdb = CedictPlusDb.CedictPlusDb(dbname) 
    rows = plusdb.fetch_dict_all(n=None)    
    for r in rows:
        print >>file, r['traditional'],  r['simplified'], '['+r['reading']+']',  '('+r['pos']+')',  '/'+r['translation']+'/'

    
def main():
    if len(sys.argv) < 2:
        print "Usage:",  sys.argv[0],  "option ...."
        print "please check you parameters. current number is %d" % (len(sys.argv) - 1)
        print "option can be 'o' or 'i' or 'e' "
        return

    option = sys.argv[1]
    if option not in ["o",  "i",  "e"]:
        print "option error"
        return
    
    if option == "o":
        cedit_parse_to_txt()
    elif option == "i":
        cedit_parse_with_dict()   
    elif option == 'e':
        export_cedictplus_txt()

if __name__ == '__main__':
    main()
    
