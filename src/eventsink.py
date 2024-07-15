##### START PROVIDED CODE #####
# This block of code is provided for you. Please utilize, but do not modify.
import random

class Socket:
    # assume this is a callback for when event messages are received from the socket
    def recv(self) -> bytes:
        messages = (
        "Source=positioner,Index=1,Data=positioner_data,Timestamp=2024-06-20T15:53:03.736+06:00",
        "Source=radar,Index=1,Data=radar_data,Timestamp=1718804476_398",
        "Source=power,Index=1,Data=power_data,Timestamp=1718804476398"
        )
        return random.choice(messages).encode()

class Database:
    # assume this stub will save event data to the database
    def save(self, source: str, index: int, data: str, timestamp: str):
        print(source, index, data, timestamp)

db = Database()
socket = Socket()

##### END PROVIDED CODE #######

##### START SOLUTION ######
# Your code below this line

from typing import List, Iterable, Dict, Optional, Callable
from dataclasses import dataclass
from enum import Enum
from collections import namedtuple
from threading import Thread
import queue
from datetime import datetime

@dataclass
class Message:
    SOURCE_KEY = "Source"
    INDEX_KEY = "Index"
    DATA_KEY = "Data"
    TIMESTAMP_KEY = "Timestamp"

    source: str
    index: int
    data: str
    timestamp: str
    
@dataclass(init=False)
class PositionerMessage(Message):
    ID: str = "positioner"
    DT_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

    @staticmethod
    def normalize_timestamp(timestamp: str) -> str:
        return str(int(datetime.strptime(timestamp, PositionerMessage.DT_FORMAT).timestamp() * 1000))

    def __init__(self, index: int, data: str, timestamp: str):
        Message.__init__(self, PositionerMessage.ID, index, data, PositionerMessage.normalize_timestamp(timestamp))

@dataclass(init=False)
class RadarMessage(Message):
    ID: str = "radar"

    @staticmethod
    def normalize_timestamp(timestamp: str) -> str:
        return timestamp.replace("_", "")
    
    def __init__(self, index: int, data: str, timestamp: str):
        Message.__init__(self, RadarMessage.ID, index, data, RadarMessage.normalize_timestamp(timestamp))

@dataclass(init=False)
class PowerMessage(Message):
    ID: str = "power"
    def __init__(self, index: int, data: str, timestamp: str):
        Message.__init__(self, PowerMessage.ID, index, data, timestamp)

class SourceType(namedtuple('SourceType', 'id message_producer'), Enum):
    POSITIONER = PositionerMessage.ID, PositionerMessage
    RADAR = RadarMessage.ID, RadarMessage
    POWER = PowerMessage.ID, PowerMessage

    @staticmethod
    def get_message_producers() -> Dict[str, Callable[[str, str, str], Message]]:
        return {type.id: type.message_producer for type in SourceType}


class SocketDatabasePipe:
    BATCH_SIZE = 10
    QUEUE_TIMEOUT_SECS = 3

    def __init__(self, socket: Socket, db: Database):
        self._socket: Socket = socket
        self._db: Database = db
        self._message_producers: Dict[str, Callable[[str, str, str], Message]] = SourceType.get_message_producers()
        self._queue: queue.Queue = queue.Queue()
        self._is_done: bool = False
        self._source_thread: Thread = Thread(target=self.__source, daemon=True)

    def __deserialize_payload(self, payload: bytes) -> Optional[Message]:
        try:
            # Could be more efficient by assuming the order of the key/value pairs, but this is able to handle out of order key/value pairs.
            fields: Dict[str, str] = dict(map(lambda field: field.split("="), payload.decode("utf-8").split(",")))
            
            producer: Optional[Callable[[str, str, str], Message]] = self._message_producers.get(fields[Message.SOURCE_KEY])
            if producer is not None:
                return producer(fields[Message.INDEX_KEY], fields[Message.DATA_KEY], fields[Message.TIMESTAMP_KEY])
            else:
                return None
        except KeyError:
            return None

    def sink(self):
        """
        This starts our reading from the socket, deserializes the received payload and pushed the message onto the queue.
        This is meant to be constantly running while a background thread is pushing queued messages into the database.
        """
        # We could add a way to gracefully stop this sink, but for simplicity it'll just loop forever until the program is interrupted.
        while True:
            payload: bytes = socket.recv()
            message: Optional[Message] = self.__deserialize_payload(payload)
            if message is not None:
                self._queue.put(message)

    def __save_messages(self, messages: Iterable[Message]):
        # This could be optimized to be a batch insert into the database just in case there's a burst of messages and the database client
        # isn't able to keep up.
        for message in messages:
            self._db.save(message.source, message.index, message.data, message.timestamp)

    def __source(self):
        """
        This listens to our message queue and saves batches of messages (10 maximum) to the database.
        It's possible that while saving something to the database there might be a build up of messages coming in
        through the socket (in reality anyways), so by saving batches of messages we're able to keep up with
        a large amount of incoming messages without the socket's buffer filling up.
        """
        messages: List[Message] = []
        while not self._is_done:
            try:
                # The first get() will wait block with a timeout and will throw an empty exception if nothing is received
                # before the timeout.
                messages.append(self._queue.get(block=True, timeout=SocketDatabasePipe.QUEUE_TIMEOUT_SECS))
                # task_done() allows the queue to be well informed about messages being processed so that its join() properly works.
                self._queue.task_done()

                # After receiving a message, we check to see if there are other messages in the queue, but we're only going to
                # get a limited number of messages at a time.
                for i in range(SocketDatabasePipe.BATCH_SIZE):
                    # Either get_nowait throws an empty exception or it gets a pending message.
                    messages.append(self._queue.get_nowait())
                    self._queue.task_done()
            except queue.Empty:
                pass

            # If there are messages, then we'll save the batch to the database.
            if len(messages) > 0:
                self.__save_messages(messages)
                messages.clear()
            

    def __enter__(self):
        """
        This starts our source thread which will push queued messages to the database in the background.
        """
        self._source_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        This provides us with a try-with-resources like behavior. When the with statement has comleted
        naturally or because of an error we will try to save the currently queued messages to the database
        and gracefully stop our source thread.
        """
        # Wait on the processing of the messages currently in the queue.
        self._queue.join()
        # Inform the source thread that it can stop, which will probably take at most 5 seconds because of the queue.get() timeout.
        self._is_done = True
        self._source_thread.join()

if __name__ == "__main__":
    # Here's where the pipe is created and started.
    with SocketDatabasePipe(socket, db) as pipe:
        pipe.sink()