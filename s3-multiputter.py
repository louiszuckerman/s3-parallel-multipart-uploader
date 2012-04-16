#!/usr/bin/python
'''
s3-multiputter.py

Description:

Reads a huge file (or device) and uploads to Amazon S3.
Multiple workers are launched which read & send in parallel.
Workers are allocated one chunk of the file at a time.

Usage:

s3-multiputter.py <BUCKET> <FILE> <THREADS> <CHUNKSIZE>

BUCKET: The S3 bucket name to upload to
FILE: The source file to upload
THREADS: Number of parallel uploader threads
CHUNKSIZE: Size (MB) of each chunk

Created on Aug 30, 2011

@author: louis zuckerman
'''

import boto
import sys
import io
import cStringIO
from multiprocessing import Process, Queue
from Queue import Empty
import time

class MPUploader(Process):
    def __init__(self, queue, donequeue, srcfile, chunksize, bucket):
        self.s3c = boto.connect_s3()
        for mpu in self.s3c.lookup(bucket).list_multipart_uploads():
            if mpu.key_name == srcfile:
                self.multipart = mpu
                break
        self.work = queue
        self.donework = donequeue
        self.infilename = srcfile
        self.chunksize = chunksize
        Process.__init__(self)
    
    def send_part(self, partno):
        while True:
            try:
                self.multipart.upload_part_from_file(self.buffer, partno+1)
                break
            except Exception as s3err:
                print "S3 ERROR IN PART ", partno, ": ", type(s3err), "\n\t", s3err, "\n\t\t(retrying)"
                continue
    
    def read_part(self, partno):
        while True:
            try:
                self.infile = open(self.infilename, "rb", 0)
                self.infile.seek(partno*self.chunksize)
                self.buffer.write(self.infile.read(self.chunksize))
                self.infile.close()
                return self.buffer.tell() #return number of bytes read, or 0 if EOF
            except Exception as ioerr:
                return 0
    
    def run(self):
        while True:
            uppart = self.work.get()
            self.buffer = cStringIO.StringIO()
            if self.read_part(uppart) == 0:
                break #past EOF
            self.send_part(uppart)
            self.buffer.close()
            self.donework.put(uppart)

if __name__ == '__main__':
    buck = sys.argv[1]
    srcfile = sys.argv[2]
    workers = int(sys.argv[3])
    chunksize = int(sys.argv[4])*1024*1024
    s3c = boto.connect_s3()
    mpu = s3c.lookup(buck).initiate_multipart_upload(srcfile)
    work = Queue()
    donework = Queue()
    
    starttime = time.clock()
    
    chunkstep = 100
    chunkserving = 0
    chunksdone = 0
    
    if (chunkserving - chunksdone) < (chunkstep / 2):
        for c in range(chunkserving, chunkserving+chunkstep):
            work.put(c)
        chunkserving += chunkstep
    
    uploader = range(workers)
    print "Launching workers ",
    for i in range(workers):
        uploader[i] = MPUploader(work, donework, srcfile, chunksize, buck)
        uploader[i].start()
        print ".",
        sys.stdout.flush()
    print len(uploader)
    
    def childrenExist(kids):
        for child in kids:
            if child.is_alive():
                return True
        return False
    
    while childrenExist(uploader):
        try:
            while True:
                try:
                    donework.get_nowait()
                    chunksdone += 1
                except Empty:
                    break
            
            if (chunkserving - chunksdone) < (chunkstep / 2):
                for c in range(chunkserving, chunkserving+chunkstep):
                    work.put(c)
                chunkserving += chunkstep
    
            print (chunksdone*chunksize)/(1024*1024), " ",
            sys.stdout.flush()
            time.sleep(1)
        except KeyboardInterrupt:
            for w in uploader:
                w.terminate()
            time.sleep(1)
            mpu.cancel_upload()
            print "ABORTED!"
            exit()
    
    complete = mpu.complete_upload()
    print "COMPLETED IN ", time.clock()-starttime, "s"
    exit()

