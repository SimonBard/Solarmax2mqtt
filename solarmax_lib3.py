#!/usr/bin/python
# -* coding: utf-8 *-

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# Developed 2022 by Simon Bard, adapted from Bernd Wurst> 
# for own use.


import socket, datetime

# Konstanten
inverter_types = {
    20010: {'desc': 'SolarMax 2000S', 'max': 2000, },  # Nur geraten
    20020: {'desc': 'SolarMax 3000S', 'max': 3000, },
    20030: {'desc': 'SolarMax 4200S', 'max': 4200, },
    20040: {'desc': 'SolarMax 6000S', 'max': 6000, },
}

query_types = ['KLD', 'KDY', 'KYR', 'KMT', 'KT0', 'IL1', 'IDC', 'PAC', 'PRL',
               'SYS', 'SAL', 'TNF', 'PDC', 'PRL', 'TKK', 'UL1', 'UDC',
               'ADR', 'TYP', 'PIN', 'MAC', 'CAC', 'KHR', 'EC00', 'EC01',
               'EC02', 'EC03', 'EC04', 'EC05', 'EC06', 'EC07', 'EC08',
               'BDN', 'SWV', 'DIN', 'LAN', 'SDAT', 'FDAT']
query_codes = {
    "IDC": "DC Current",
    "UL1": "Voltage_Phase_1",
    "TKK": "Inverter_Operating_temp",
    "IL1": "Current_phase_1",
    "SYS": "Operation_State",
    "TNF": "generated_frequency_(Hz)",
    "UDC": "DC_voltage_(VDC)",
    "PAC": "AC_power_being_generated",
    "PRL": "relative_output_(%)",
    "KT0": "total_yield_(kWh)",
    "KDY": "Yield_today_(kWh)_Tagesertrag",
    "KYR": "Energy_this_Year_(kWh)",
    "KMT": "Energy_ThisYear",
    "KLD": "Energy_LastDay"
}

"""
array( 'descr' => 'Address',                   'name' => 'ADR', 'convert' => function($i){ return hexdec($i); } ), # 0
array( 'descr' => 'Type',                      'name' => 'TYP', 'convert' => function($i) { return "0x" . $i; } ), # 1
array( 'descr' => 'Software version',          'name' => 'SWV', 'convert' => function($i){ return sprintf("%1.1f", hexdec($i) / 10 ); } ), # 2
array( 'descr' => 'Date day',                  'name' => 'DDY', 'convert' => function($i){ return hexdec($i); } ), # 3
array( 'descr' => 'Date month',                'name' => 'DMT', 'convert' => function($i){ return hexdec($i); } ), # 4
array( 'descr' => 'Date year',                 'name' => 'DYR', 'convert' => function($i){ return hexdec($i); } ), # 5
array( 'descr' => 'Time hours',                'name' => 'THR', 'convert' => function($i){ return hexdec($i); } ), # 6
array( 'descr' => 'Time minutes',              'name' => 'TMI', 'convert' => function($i){ return hexdec($i); } ), # 7
array( 'descr' => '???Error 1, number???',     'name' => 'E11', 'convert' => function($i){ return hexdec($i); } ), # 8
array( 'descr' => '???Error 1, day???',        'name' => 'E1D', 'convert' => function($i){ return hexdec($i); } ), # 9
array( 'descr' => '???Error 1, month???',      'name' => 'E1M', 'convert' => function($i){ return hexdec($i); } ), # 10
array( 'descr' => '???Error 1, hour???',       'name' => 'E1h', 'convert' => function($i){ return hexdec($i); } ), # 11
array( 'descr' => '???Error 1, minute???',     'name' => 'E1m', 'convert' => function($i){ return hexdec($i); } ), # 12
array( 'descr' => '???Error 2, number???',     'name' => 'E21', 'convert' => function($i){ return hexdec($i); } ), # 13
array( 'descr' => '???Error 2, day???',        'name' => 'E2D', 'convert' => function($i){ return hexdec($i); } ), # 14
array( 'descr' => '???Error 2, month???',      'name' => 'E2M', 'convert' => function($i){ return hexdec($i); } ), # 15
array( 'descr' => '???Error 2, hour???',       'name' => 'E2h', 'convert' => function($i){ return hexdec($i); } ), # 16
array( 'descr' => '???Error 2, minute???',     'name' => 'E2m', 'convert' => function($i){ return hexdec($i); } ), # 17
array( 'descr' => '???Error 3, number???',     'name' => 'E31', 'convert' => function($i){ return hexdec($i); } ), # 18
array( 'descr' => '???Error 3, day???',        'name' => 'E3D', 'convert' => function($i){ return hexdec($i); } ), # 19
array( 'descr' => '???Error 3, month???',      'name' => 'E3M', 'convert' => function($i){ return hexdec($i); } ), # 20
array( 'descr' => '???Error 3, hour???',       'name' => 'E3h', 'convert' => function($i){ return hexdec($i); } ), # 21
array( 'descr' => '???Error 3, minute???',     'name' => 'E3m', 'convert' => function($i){ return hexdec($i); } ), # 22
array( 'descr' => 'Operating hours',           'name' => 'KHR', 'convert' => function($i){ return hexdec($i); } ), # 23
array( 'descr' => 'Energy today [Wh]',         'name' => 'KDY', 'convert' => function($i){ return (hexdec($i) * 100); } ), # 24
array( 'descr' => 'Energy yesterday [kWh]',    'name' => 'KLD', 'convert' => function($i){ return (hexdec($i) * 100); } ), # 25
array( 'descr' => 'Energy this month [kWh]',   'name' => 'KMT', 'convert' => function($i){ return hexdec($i); } ), # 26
array( 'descr' => 'Energy last monh [kWh]',    'name' => 'KLM', 'convert' => function($i){ return hexdec($i); } ), # 27
array( 'descr' => 'Energy this year [kWh]',    'name' => 'KYR', 'convert' => function($i){ return hexdec($i); } ), # 28
array( 'descr' => 'Energy last year [kWh]',    'name' => 'KLY', 'convert' => function($i){ return hexdec($i); } ), # 29
array( 'descr' => 'Energy total [kWh]',        'name' => 'KT0', 'convert' => function($i){ return hexdec($i); } ), # 30
array( 'descr' => 'Language',                  'name' => 'LAN', 'convert' => function($i){ return hexdec($i); } ), # 31
array( 'descr' => 'DC voltage [mV]',           'name' => 'UDC', 'convert' => function($i){ return (hexdec($i) * 100); } ), # 32
array( 'descr' => 'AC voltage [mV]',           'name' => 'UL1', 'convert' => function($i){ return (hexdec($i) * 100); } ), # 33
array( 'descr' => 'DC current [mA]',           'name' => 'IDC', 'convert' => function($i){ return (hexdec($i) * 10); } ), # 34
array( 'descr' => 'AC current [mA]',           'name' => 'IL1', 'convert' => function($i){ return (hexdec($i) * 10); } ), # 35
array( 'descr' => 'AC power [mW]',             'name' => 'PAC', 'convert' => function($i){ return (hexdec($i) * 500); } ), # 36
array( 'descr' => 'Power installed [mW]',      'name' => 'PIN', 'convert' => function($i){ return (hexdec($i) * 500); } ), # 37
array( 'descr' => 'AC power [%]',              'name' => 'PRL', 'convert' => function($i){ return hexdec($i); } ), # 38
array( 'descr' => 'Start ups',                 'name' => 'CAC', 'convert' => function($i){ return hexdec($i); } ), # 39
array( 'descr' => '???',                       'name' => 'FRD', 'convert' => function($i){ return "0x" . $i; } ), # 40
array( 'descr' => '???',                       'name' => 'SCD', 'convert' => function($i){ return "0x" . $i; } ), # 41
array( 'descr' => '???',                       'name' => 'SE1', 'convert' => function($i){ return "0x" . $i; } ), # 42
array( 'descr' => '???',                       'name' => 'SE2', 'convert' => function($i){ return "0x" . $i; } ), # 43
array( 'descr' => '???',                       'name' => 'SPR', 'convert' => function($i){ return "0x" . $i; } ), # 44
array( 'descr' => 'Temerature Heat Sink',      'name' => 'TKK', 'convert' => function($i){ return hexdec($i); } ), # 45
array( 'descr' => 'AC Frequency',              'name' => 'TNF', 'convert' => function($i){ return (hexdec($i) / 100); } ), # 46
array( 'descr' => 'Operation State',           'name' => 'SYS', 'convert' => function($i){ return hexdec($i); } ), # 47
array( 'descr' => 'Build number',              'name' => 'BDN', 'convert' => function($i){ return hexdec($i); } ), # 48
array( 'descr' => 'Error-Code(?) 00',          'name' => 'EC00', 'convert' => function($i){ return hexdec($i); } ), # 49
array( 'descr' => 'Error-Code(?) 01',          'name' => 'EC01', 'convert' => function($i){ return hexdec($i); } ), # 50
array( 'descr' => 'Error-Code(?) 02',          'name' => 'EC02', 'convert' => function($i){ return hexdec($i); } ), # 51
array( 'descr' => 'Error-Code(?) 03',          'name' => 'EC03', 'convert' => function($i){ return hexdec($i); } ), # 52
array( 'descr' => 'Error-Code(?) 04',          'name' => 'EC04', 'convert' => function($i){ return hexdec($i); } ), # 53
array( 'descr' => 'Error-Code(?) 05',          'name' => 'EC05', 'convert' => function($i){ return hexdec($i); } ), # 54
array( 'descr' => 'Error-Code(?) 06',          'name' => 'EC06', 'convert' => function($i){ return hexdec($i); } ), # 55
array( 'descr' => 'Error-Code(?) 07',          'name' => 'EC07', 'convert' => function($i){ return hexdec($i); } ), # 56
array( 'descr' => 'Error-Code(?) 08',          'name' => 'EC08', 'convert' => function($i){ return hexdec($i); } ), # 57
 """

status_codes = {
    20000: 'No Communication', 20001: 'Running', 20002: 'Irradiance too low', 20003: 'Startup', 20004: 'MPP operation',
    20006: 'Maximum power',
    20007: 'Temperature limitation', 20008: 'Mains operation', 20009: 'Idc limitation', 20010: 'Iac limitation',
    20011: 'Test mode', 20012: 'Remote controlled', 20013: 'Restart delay', 20014: 'External limitation',
    20015: 'Frequency limitation', 20016: 'Restart limitation', 20017: 'Booting', 20018: 'Insufficient boot power',
    20019: 'Insufficient power', 20021: 'Uninitialized', 20022: 'Disabled', 20023: 'Idle', 20024: 'Powerunit not ready',
    20050: 'Program firmware', 20101: 'Device error 101', 20102: 'Device error 102', 20103: 'Device error 103',
    20104: 'Device error 104', 20105: 'Insulation fault DC', 20106: 'Insulation fault DC', 20107: 'Device error 107',
    20108: 'Device error 108', 20109: 'Vdc too high', 20110: 'Device error 110', 20111: 'Device error 111',
    20112: 'Device error 112',
    20113: 'Device error 113', 20114: 'Ierr too high', 20115: 'No mains', 20116: 'Frequency too high',
    20117: 'Frequency too low',
    20118: 'Mains error', 20119: 'Vac 10min too high', 20120: 'Device error 120', 20121: 'Device error 121',
    20122: 'Vac too high',
    20123: 'Vac too low', 20124: 'Device error 124', 20125: 'Device error 125', 20126: 'Error ext. input 1',
    20127: 'Fault ext. input 2',
    20128: 'Device error 128', 20129: 'Incorr. rotation dir.', 20130: 'Device error 130', 20131: 'Main switch off',
    20132: 'Device error 132',
    20133: 'Device error 133', 20134: 'Device error 134', 20135: 'Device error 135', 20136: 'Device error 136',
    20137: 'Device error 137',
    20138: 'Device error 138', 20139: 'Device error 139', 20140: 'Device error 140', 20141: 'Device error 141',
    20142: 'Device error 142',
    20143: 'Device error 143', 20144: 'Device error 144', 20145: 'df/dt too high', 20146: 'Device error 146',
    20147: 'Device error 147',
    20148: 'Device error 148', 20150: 'Ierr step too high', 20151: 'Ierr step too high', 20153: 'Device error 153',
    20154: 'Shutdown 1',
    20155: 'Shutdown 2', 20156: 'Device error 156', 20157: 'Insulation fault DC', 20158: 'Device error 158',
    20159: 'Device error 159',
    20160: 'Device error 160', 20161: 'Device error 161', 20163: 'Device error 163', 20164: 'Ierr too high',
    20165: 'No mains',
    20166: 'Frequency too high', 20167: 'Frequency too low', 20168: 'Mains error', 20169: 'Vac 10min too high',
    20170: 'Device error 170',
    20171: 'Device error 171', 20172: 'Vac too high', 20173: 'Vac too low', 20174: 'Device error 174',
    20175: 'Device error 175',
    20176: 'Error DC polarity', 20177: 'Device error 177', 20178: 'Device error 178', 20179: 'Device error 179',
    20180: 'Vdc too low',
    20181: 'Blocked external', 20185: 'Device error 185', 20186: 'Device error 186', 20187: 'Device error 187',
    20188: 'Device error 188',
    20189: 'L and N interchanged', 20190: 'Below-average yield', 20191: 'Limitation error', 20198: 'Device error 198',
    20199: 'Device error 199',
    20999: 'Device error 999'
}

alarm_codes = {
    0: 'kein Fehler',
    1: 'Externer Fehler 1',
    2: 'Isolationsfehler DC-Seite',
    4: 'Fehlerstrom Erde zu Groß',
    8: 'Sicherungsbruch Mittelpunkterde',
    16: 'Externer Alarm 2',
    32: 'Langzeit-Temperaturbegrenzung',
    64: 'Fehler AC-Einspeisung',
    128: 'Externer Alarm 4',
    256: 'Ventilator defekt',
    512: 'Sicherungsbruch',
    1024: 'Ausfall Temperatursensor',
    2048: 'Alarm 12',
    4096: 'Alarm 13',
    8192: 'Alarm 14',
    16384: 'Alarm 15',
    32768: 'Alarm 16',
    65536: 'Alarm 17',
}


# Hilfs-Routine (DEBUG)

def DEBUG(*s):
    # type: (object) -> object
    out = [datetime.datetime.now().isoformat() + ':', ] + [str(x) for x in s]
    print(' '.join(out))


####################################
## Haupt-Klasse
####################################


class SolarMax(object):
    def __init__(self, host, port):
        self.__host = host
        self.__port = port
        self.__inverters = {}
        self.__socket = None
        self.__connected = False
        self.__allinverters = False
        self.__inverter_list = []
        self.__connect()

    def __repr__(self):
        return 'SolarMax[%s:%s / socket=%s]' % (self.__host, self.__port, self.__socket)

    def __str__(self):
        return 'SolarMax[%s:%s / socket=%s / inverters=%s]' % (
            self.__host, self.__port, self.__socket, self.inverters())

    def __disconnect(self):
        try:
            DEBUG('Closing open connection to %s:%s' % (self.__host, self.__port))
            self.__socket.shutdown(socket.SHUT_RDWR)
            self.__socket.close()
            del self.__socket
        except:
            pass
        finally:
            self.__connected = False
            self.__allinverters = False
            self.__socket = None

    def __del__(self):
        DEBUG('destructor called')
        self.__disconnect()

    def __connect(self):
        self.__disconnect()
        DEBUG('establishing connection to %s:%i...' % (self.__host, self.__port))
        try:
            self.__socket = socket.socket()
            s = self.__socket
            s.settimeout(5)
            s.connect((self.__host, self.__port))
            s.settimeout(10)
            self.__connected = True
            DEBUG('connected.')
        except:
            DEBUG('connection to %s:%i failed' % (self.__host, self.__port))
            self.__connected = False
            self.__allinverters = False

    # Utility-functions
    def hexval(self, i):
        return (hex(i)[2:]).upper()

    def checksum(self, s):
        total = 0
        for c in s:
            total += ord(c)
        h = self.hexval(total)
        while len(h) < 4:
            h = '0' + h
        return h

    def __receive(self):
        DEBUG('Receiving data...')
        try:
            data = ''
            tmp = ''
            while True:
                tmp = self.__socket.recv(1)
                # socket.recv returns bytes
                tmp = tmp.decode()
                # DEBUG(tmp)
                data += tmp
                if len(tmp) < 1 or tmp == '}':
                    break
                tmp = ''
            return data
        except Exception as e:
            DEBUG(e)
            self.__allinverters = False
            return ""

    def __parse(self, answer):
        # convenience checks
        if answer[0] != '{' or answer[-1] != '}':
            raise ValueError('malformed answer: %s' % answer)
        raw_answer = answer
        # p.ex. {01;FB;67|64:KLD=15;PAC=3E;KDY=A;KT0=4B35;IDC=11;UDC=A72;IL1=2D;UL1=904;FDAT=7DD0613,0;SYS=4E28,0|17E5}
        answer = answer[1:-1]  # get rid of { and }
        checksum = answer[-4:]
        content = answer[:-4]
        # checksum
        # if checksum != self.checksum(content):
        # raise ValueError('checksum error')

        (header, content) = content[:-1].split('|', 2)
        (inverter, fb, length) = header.split(';', 3)
        if fb != 'FB':
            raise ValueError('answer not understood')
        # length
        length = int(length, 16)
        if length != len(raw_answer):
            raise ValueError('length mismatch')

        inverter = int(inverter)

        # Bei schreibzugriff antwortet der WR mit 'C8'
        # if not content.startswith('64:'):
        #  raise ValueError('Inverter did not understand our query')

        content = content[3:]
        data = {}

        for item in content.split(';'):
            (key, value) = item.split('=')
            if key not in query_types:
                raise NotImplementedError("Don't know %s" % item)
            data[key] = value
        return (inverter, data)

    def __build_query(self, id, values, qtype=100):
        qtype = self.hexval(qtype)
        if type(values) == list:
            for v in values:
                if v not in query_types:
                    raise ValueError('Unknown data type »' + v + '«')
            values = ';'.join(values)
        elif type(values) in [str, unicode]:
            pass
        else:
            raise ValueError('value has unsupported type')

        # The querystring needs to be encoded to bytes in python 3
        querystring = '|' + qtype + ':' + values + '|'
        # Länge vergrößern um: 2 x { (2), WR-Nummer (2), "FB" (2), zwei Semikolon (2), Länge selbst (2), checksumme (4)
        l = len(querystring) + 2 + 2 + 2 + 2 + 2 + 4
        querystring = 'FB;%02i;%s%s' % (int(id), self.hexval(l), querystring)
        querystring += self.checksum(querystring)
        querystring = '{' + querystring + '}'  # is needed to send it as bytes
        querystring = querystring.encode()
        return querystring
        # return b'{FB;01;1E|64:ADR;TYP;PIN|06A2}'

    def __send_query(self, querystring):
        try:
            DEBUG('Called send_query')
            DEBUG(self.__host, '=>', querystring)
            # DEBUG(self.__host, '=>', b'{FB;01;1E|64:ADR;TYP;PIN|06A2}', '<= this would be correct')
            self.__socket.send(querystring)

        except socket.timeout as e:
            DEBUG(e)
            self.__allinverters = False
        except socket.error as e:
            DEBUG(e)
            self.__connected = False
        except Exception as e:
            DEBUG('error while sending query: ', e)

    def query(self, id, values, qtype=100):
        q = self.__build_query(id, values, qtype)
        DEBUG("Trying to send this query to Inverter no %i: %s" % (id, q))
        self.__send_query(q)
        answer = self.__receive()
        DEBUG('Received the following data: ', answer)
        if answer:
            (inverter, data) = self.__parse(answer)
            for d in data.keys():
                data[d] = self.normalize_value(d, data[d])
            return (inverter, data)
        else:
            self.__allinverters = False

        if not self.__allinverters and not self.__detection_running:
            self.detect_inverters()
        elif not self.__connected:
            self.__connect()
        else:
            DEBUG('Timeout while connecting')
            raise socket.timeout

        return None

    def normalize_value(self, key, value):
        if key in ['KDY', 'UL1', 'UDC', 'KMT', 'KYR']:
            return float(int(value, 16)) / 10
        elif key in ['IL1', 'IDC', 'TNF', ]:
            return float(int(value, 16)) / 100
        elif key in ['KLM', 'KLY', 'KLD', ]:
            return float(int(value, 16)) / 10
        elif key in ['PAC', 'PIN', ]:
            return float(int(value, 16)) / 2
        elif key in ['SAL', ]:
            return int(value, 16)
        elif key in ['SYS', ]:
            (x, y) = value.split(',', 2)
            x = int(x, 16)
            y = int(y, 16)
            return (x, y)
        elif key in ['SDAT', 'FDAT']:
            (date, time) = value.split(',', 2)
            time = int(time, 16)
            return datetime.datetime(int(date[:3], 16), int(date[3:5], 16), int(date[5:], 16), 0, 0, 0)
        else:
            return int(value, 16)

    def write_setting(self, inverter, data):
        rawdata = []
        for key, value in data.items():
            key = key.upper()
            if key not in query_types:
                raise ValueError('unknown type')
            value = self.hexval(value)
            rawdata.append('%s=%s' % (key, value))
        DEBUG(self.query(inverter, ';'.join(rawdata), 200))

    def status(self, inverter):
        result = self.query(inverter, ['SYS', 'SAL'])
        if not result:
            return ('Offline', 'Offline')
        result = result[1]
        errors = []
        if result['SAL'] > 0:
            for (code, descr) in alarm_codes.items():
                if code & result['SAL']:
                    errors.append(descr)

        status = status_codes[result['SYS'][0]]
        return (status, ', '.join(errors))

    def use_inverters(self, list_of):
        self.__inverter_list = list_of
        self.detect_inverters()

    def detect_inverters(self):
        self.__inverters = {}
        if not self.__connected:
            self.__connect()
        self.__detection_running = True
        for inverter in self.__inverter_list:
            DEBUG('searching for #%i (socket: %s)' % (inverter, self.__socket))
            try:
                DEBUG('searching for #%i (socket: %s)' % (inverter, self.__socket))
                (inverter, data) = self.query(inverter, ['ADR', 'TYP', 'PIN'])
                if data['TYP'] in inverter_types.keys():
                    self.__inverters[inverter] = inverter_types[data['TYP']].copy()
                    self.__inverters[inverter]['installed'] = data['PIN']
                    DEBUG('Inverter found')
                else:
                    DEBUG('Unknown inverter type: %s (ID #%i)' % (data['TYP'], data['ADR']))
            except:
                DEBUG('Inverter #%i not found' % inverter)
                self.__allinverters = False
        self.__detection_running = False
        if len(self.__inverters) == len(self.__inverter_list):
            self.__allinverters = True
            DEBUG('found all inverters:')
            DEBUG(self.__inverters)
        else:
            DEBUG('not all invertes found, reconnection!')
            DEBUG('found %i of %i' % (len(self.__inverters), len(self.__inverter_list)))
            self.__connect()

    def inverters(self):
        if not self.__allinverters:
            self.detect_inverters()
        return self.__inverters
