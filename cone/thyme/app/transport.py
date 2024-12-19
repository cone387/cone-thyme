import queue
from kombu.transport.virtual import Transport
from kombu.transport import get_transport_cls


class QueueTransport(Transport):
    """自定义 Kombu Transport，使用 Python 的 queue.Queue 来传递消息"""
    driver_type = 'queue'
    driver_name = 'queue'

    def __init__(self, client, queue_instance=None):
        # 如果没有传入 queue.Queue 实例，则使用默认的 queue.Queue
        self.queue_instance = queue_instance or queue.Queue()
        super(QueueTransport, self).__init__(client)

    # def create_channel(self, connection):
    #     return connection

    # def _get_connection(self):
    #     # 在这里，我们的传输层不需要真正的网络连接
    #     # 直接返回自身即可
    #     return self.client
    #
    # def establish_connection(self):
    #     return self.client

    def _put(self, *args, **kwargs):
        """将消息放入 queue.Queue"""
        self.queue_instance.put({'a': 'bc'})

    def _get(self):
        """从 queue.Queue 获取消息"""
        try:
            return self.queue_instance.get_nowait()  # 非阻塞地获取消息
        except queue.Empty:
            return None

    def close(self):
        """关闭传输连接，这里不需要做什么特别的处理"""
        pass
