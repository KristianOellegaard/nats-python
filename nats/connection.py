import re
import json
from twisted.internet.protocol import Protocol, Factory

class NATSProtocol(Protocol):
    PORT = 4222
    MAX_CONTROL_LINE_SIZE = 512

    # Parser state
    AWAITING_CONTROL = 0
    AWAITING_MSG_PAYLOAD = 1

    # Reconnect Parameters, 2 sec wait, 10 tries
    RECONNECT_TIME_WAIT = 2*1000
    MAX_RECONNECT_ATTEMPTS = 10

    # Protocol
    CONTROL_LINE = re.compile(r'^(.*)\r\n')
    MSG = re.compile(r'^MSG\s+([^\s\r\n]+)\s+([^\s\r\n]+)\s+(([^\s\r\n]+)[^\S\r\n]+)?(\d+)\r\n')
    OK = re.compile(r'^\+OK\s*\r\n')
    ERR = re.compile(r"^-ERR\s+('.+')?\r\n")
    PING = re.compile(r'^PING\r\n')
    PONG = re.compile(r'^PONG\r\n')
    INFO = re.compile(r'^INFO\s+([^\r\n]+)\r\n')

    CR_LF = '\r\n'
    CR_LF_LEN = len(CR_LF)
    EMPTY = ''
    SPC = ' '

    PUBLISH = 'PUB'
    SUBSCRIBE = 'SUB'
    UNSUBSCRIBE = 'UNSUB'
    CONNECT = 'CONNECT'

    # Responses

    PING_REQUEST = 'PING' + CR_LF
    PONG_RESPONSE = "PONG" + CR_LF

    # Pedantic Mode support

    Q_SUB = r'^([^\.\*>\s]+|>$|\*)(\.([^\.\*>\s]+|>$|\*))*$'
    Q_SUB_NO_WC = r'^([^\.\*>\s]+)(\.([^\.\*>\s]+))*$'

    def __init__(self):
        self.pState = self.AWAITING_CONTROL
        self.sid = 0
        self.subs = {}

    def _send_command(self, command):
        self.transport.write(command)

    def ping(self):
        self._send_command(self.PING_REQUEST)

    def subscribe(self, pattern, f=None):
        self.sid += 1
        self._send_command(self.SPC.join((
               self.SUBSCRIBE, pattern, str(self.sid)
            ))
        )
        self.subs[str(self.sid)] = {'callback': f, 'msg': None, 'reply': None}

    def publish(self, channel, message):
        # TODO: Seems to follow protocol, but doesn't work completely
        self._send_command(self.SPC.join((
            self.PUBLISH, channel, len(message),self.CR_LF,message, self.CR_LF, message
            ))
        )

    def connect(self, username, password):
        self._send_command(self.CONNECT + self.SPC + json.dumps(
            {
                'verbose': False,
                'pedantic': False,
                'user': username,
                'pass': password,
            }
        ) + self.CR_LF)
        return True

    def dataReceived(self, data):
        if self.pState == self.AWAITING_CONTROL:
            if self.MSG.match(data):
                msg = self.MSG.match(data).group
                self.payload = {
                    'subj': msg(1),
                    'sid': msg(2).replace("PING", "").replace("PONG", ""),
                    'reply': msg(4),
                    'size': int(data.split("\n")[0].split()[-1]) + self.CR_LF_LEN,
                    'size_progress': 0,
                    'body': ""
                }
                self.pState = self.AWAITING_MSG_PAYLOAD
                self.processMessage(self.MSG.sub("", data))
            elif self.OK.match(data):
                pass # Ignore for now
            elif self.ERR.match(data):
                raise Exception(data)
            elif self.PONG.match(data):
                pass
            elif self.PING.match(data):
                self._send_command(self.PONG_RESPONSE)
            elif self.INFO.match(data):
                print "INFO: ", data
                pass # Ignore for now
            else:
                print "===Weird line: ", data
                pass #print "EOL"
        elif self.pState == self.AWAITING_MSG_PAYLOAD:
            self.processMessage(data)

    def processMessage(self, data):
        diff_size = self.payload['size'] - self.payload['size_progress'] + self.CR_LF_LEN # Difference in size e.g. the remaining text
        removed_content = None
        if len(data) > diff_size:
            removed_content = data[diff_size-2:len(data)] # TODO: Dunno why -2, but apparently it works! Should be implemented above as well, I guess.
            data = data[0:diff_size]
        self.payload['body'] += data
        self.payload['size_progress'] = len(self.payload['body'])
        if self.payload['size_progress'] == self.payload['size']:
            self.pState = self.AWAITING_CONTROL
        elif self.payload['size_progress'] > self.payload['size']:
            self.pState = self.AWAITING_CONTROL
        if self.subs[self.payload['sid']]['callback']:
            self.subs[self.payload['sid']]['callback'](self.payload['subj'], self.payload['body'])
        if removed_content:
            self.dataReceived(removed_content)

class NATSClientFactory(Factory):
    protocol = NATSProtocol

    def startedConnecting(self, connector):
        print 'Started to connect.'

    def clientConnectionLost(self, connector, reason):
        print 'Lost connection.  Reason:', reason

    def clientConnectionFailed(self, connector, reason):
        print 'Connection failed. Reason:', reason