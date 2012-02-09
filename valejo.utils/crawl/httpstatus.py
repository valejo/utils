'''
Created on Feb 9, 2012

@author: Tre Jones
'''
import Queue
import threading
import urllib2
import csv

class StatusGetter(threading.Thread):
    def __init__(self, url):
        self.url = url
        self.result = None
        threading.Thread.__init__(self)
    
    def get_status(self):
        return self.result
    
    def run(self):
        try:
            conn = urllib2.urlopen(self.url)
            self.result = [self.url, conn.getcode()]
            conn.close()
        except urllib2.HTTPError, e:
            self.result = [self.url, e.code]

def get_statuses(urls):
    def producer(queue, urls):
        for url in urls:
            thread = StatusGetter(url)
            thread.start()
            queue.put(thread, True)
    
    finished = []
    def consumer(queue, total_urls):
        while len(finished) < total_urls:
            thread = queue.get(True)
            thread.join()
            r = thread.get_status()
            finished.append(r)
            print len(finished), r[0]
    
    queue = Queue.Queue(30)
    prod_thread = threading.Thread(target=producer, args=(queue, urls))
    cons_thread = threading.Thread(target=consumer, args=(queue, len(urls)))
    prod_thread.start()
    cons_thread.start()
    prod_thread.join()
    cons_thread.join()
    
    return finished

def csv_httpstatus(read_path, write_path, column):
    reader = csv.reader(open(read_path, 'r'), dialect='excel')
    
    urls = []
    for line in reader:
        urls.append(line[column])
    
    results = get_statuses(urls)
    writer = csv.writer(open(write_path, 'wb'), dialect='excel')
    writer.writerows(results)
    
csv_httpstatus('c:/scripts/output.csv', 'c:/scripts/output2.csv', 0)