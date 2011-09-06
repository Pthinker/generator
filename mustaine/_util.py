import collections

class BufferedReader(object):

    def __init__(self, input_stream, buffer_size=65535):
        self._stream = input_stream
        self._limit  = buffer_size
        self._buffer = collections.deque()
        self._length = 0

    def read(self, bytes):
        result = self.peek(bytes)
        self.skip(bytes)
        return result

    def write(self, data):
        self._buffer.append(data)
        self._length += len(data)

    def peek(self, bytes=1, offset=0):
        if bytes > self._length:
            self.write(self._stream.read(max(self._limit, bytes) - self._length))

        result = str()
        for piece in self._buffer:
            size = len(piece)

            if offset > size:
                # chunk begins in another piece...
                offset -= size
                continue

            elif offset > 0:
                # chunk begins in this piece
                last_byte = offset + bytes
                
                if size > last_byte:
                    # chunk also ends in this piece
                    result += piece[offset:last_byte]
                    return result
                else:
                    # chunk spans into next piece...
                    result += piece[offset:]
                    bytes  -= (size - offset)
                    offset  = 0
                    continue

            elif bytes > size:
                # grab non-terminal chunk...
                result += piece
                bytes  -= size
                continue

            else:
                # grab terminal chunk
                result += piece[:bytes]
                return result

    def skip(self, bytes=1):
        if bytes > self._length:
            # easier to GC the buffer than to iterate
            self._buffer.clear()
            self._length = 0
            return

        self._length -= bytes
        while self._buffer:
            size = len(self._buffer[0])
            
            if size > bytes:
                # if "truncate" means to cut from the end,
                # what do you call cutting from the beginning?
                self._buffer[0] = self._buffer[0][bytes:]
                return
            else:
                self._buffer.popleft()
                bytes -= size

