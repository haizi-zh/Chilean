# encoding=utf-8

from core.processor_watcher import ProcessorWatcher
from core.oplog_watcher import OplogWatcher
from threading import Thread
from watchdog.observers import Observer
from core.msg_handler import MsgHandler




def main():
    '''
    msg_handler = MsgHandler()
    msg_queue = msg_handler.get_msg_queue()#得到消息队列
    print msg_queue
    '''

    #process_watcher = ProcessorWatcher(queue=msg_queue)
    #msg_handler_thread = Thread(target=msg_handler.process_msg)
    # worker_op_log.setDaemon(True)
    # worker_process.setDaemon(True)
    #msg_handler_thread.start()

    msg_handler = MsgHandler()
    msg_queue = msg_handler.get_msg_queue()#得到消息队列

    op_log_watcher = OplogWatcher(profile='mongo', queue=msg_queue)

    worker_op_log = Thread(target=op_log_watcher.op_info_generator)
    msg_handler_thread = Thread(target=msg_handler.process_msg)

    worker_op_log.start()
    msg_handler_thread.start()

    # msg_handler.process_msg() # wrong, should use thread
    event_handler = ProcessorWatcher(queue=msg_queue)
    observer = Observer()
    observer.schedule(event_handler, event_handler.path, recursive=True)
    observer.start()

if __name__ == '__main__':
    main()













