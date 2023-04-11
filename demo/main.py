import time

def run():
    print('inside run')
    time.sleep(15)
    raise Exception("this is an exception")

run()