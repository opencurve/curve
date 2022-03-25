import threading, traceback, sys

class runThread(threading.Thread):
    def __init__(self, funcName, *args):
        threading.Thread.__init__(self)
        self.args = args
        self.funcName = funcName
        self.exitcode = 0
        self.exception = None
        self.exc_traceback = ''


    def run(self):  # Overwrite run() method, put what you want the thread do here
        try:
            self._run()
        except Exception as e:
            self.exitcode = 1
            self.exception = e
            self.exc_traceback = ''.join(traceback.format_exception(*sys.exc_info()))
            assert  False,"exitcode is %s,error message is %s"%(self.exitcode,self.exc_traceback)

    def _run(self):
        try:
            self.result = self.funcName(*(self.args))
        except Exception as e:
            raise e
    def get_result(self):
        threading.Thread.join(self)
        try:
            return self.result
        except Exception:
            return -1


