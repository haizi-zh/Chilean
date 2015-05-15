# by bxm
FROM python:2.7.9
RUN pip install pymongo==2.8
RUN pip install pika
RUN pip  install  pyyaml 
ADD Chilean.tar /
RUN ln -s /Chilean/consumer.py consumer.py
RUN ln -s /Chilean/main.py producer.py
#CMD ["python","/Chilean/consumer.py"]

