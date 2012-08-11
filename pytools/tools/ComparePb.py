'''
Created on Aug 11, 2012

@author: carlzhou
'''
import sqlite3
import csv, codecs, cStringIO
import os

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f", 
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([unicode(s).encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)
    
def invoke_diff_merge(old_file, new_file):
    command = "/Applications/DiffMerge.app/Contents/MacOS/DiffMerge " + old_file + " " + new_file
    os.system(command)

def export_table(db_path, table_name, order_by = None):
    sql = 'select * from ' + table_name
    if order_by is not None:
        sql = sql + " order by " + order_by
    connection = sqlite3.connect(db_path)
    c = connection.cursor()
    c.execute(sql)
    cvs_path = db_path + "_" + table_name
    writer = UnicodeWriter(open(cvs_path, "wb"))
    writer.writerows(c)
    return cvs_path
    
if __name__ == '__main__':
    
    db1_path = '../files/db_compare/roamingpb.dat'
    db2_path = '../files/db_compare/roamingpb2.dat'
    
    db1_cvs_path = export_table(db1_path, 'Customer', 'SSID')
    db2_cvs_path = export_table(db2_path, 'Customer', 'SSID')
    
    invoke_diff_merge(db1_cvs_path, db2_cvs_path)
    
    