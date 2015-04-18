# encoding=utf-8

from threading import Thread

from core.oplog_watcher import OplogWatcher
from core.trigger import Trigger
from utils import serialize
from utils import deserialize


def main():
    """
    msg_handler = MsgHandler()
    msg_queue = msg_handler.get_msg_queue()#得到消息队列
    print msg_queue
    """

    # process_watcher = ProcessorWatcher(queue=msg_queue)
    # msg_handler_thread = Thread(target=msg_handler.process_msg)
    # worker_op_log.setDaemon(True)
    # worker_process.setDaemon(True)
    #msg_handler_thread.start()

    # msg_handler = MsgHandler()
    # msg_queue = msg_handler.get_msg_queue()#得到消息队列
    #msg_queue = Enqueue()


    # trigger = Trigger()
    # trigger_thread = Thread(target=trigger.process_message)
    # trigger_thread.start()

    op_log_watcher = OplogWatcher(profile='mongo-raw')
    op_log_watcher.op_info_generator()

    # watcher_thread = Thread(target=op_log_watcher.op_info_generator)
    # watcher_thread.start()
    #op_log_watcher = OplogWatcher(profile='mongo-raw')
    #op_log_watcher.op_info_generator()

    #op_log_watcher.op_info_generator()


    # msg_handler_thread = Thread(target=msg_handler.process_msg)


    # msg_handler_thread.start() # 打开一个线程,等待（block）注册或处理事件

    # # msg_handler.process_msg() # wrong, should use thread
    # event_handler = ProcessorWatcher(queue=msg_queue)
    # observer = Observer()
    # observer.schedule(event_handler, event_handler.path, recursive=True)
    # observer.start() #监控processor中文件的变化


if __name__ == '__main__':
    main()













