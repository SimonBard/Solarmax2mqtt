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

# Developed 2021 by Simon Bard>
# for own use.
# Released to the public in 2022.


import time, datetime
import paho.mqtt.client as paho

broker = "192.168.103.44"
port = 1883

query_types = ['KLD', 'PAC', 'KDY', 'KT0', 'IDC', 'UDC', 'IL1', 'UL1', 'FDAT', 'SYS']
query_codes = {
    'IDC': 'DC Current',
    'FDAT': 'Factory_Date',
    'UL1': 'Voltage_Phase_1',
    'TKK': 'Inverter_Operating_temp',
    'IL1': 'Current_phase_1',
    'SYS': 'Operation_State',
    'TNF': 'generated_frequency_(Hz)',
    'UDC': 'DC_voltage_(VDC)',
    'PAC': 'AC_power_being_generated',
    'PRL': 'relative_output_(%)',
    'KT0': 'total_yield_(kWh)',
    'KDY': 'Yield_today_(kWh)_Tagesertrag',
    'KYR': 'Energy_this_Year_(kWh)',
    'KMT': 'Energy_ThisYear',
    'KLD': 'Energy_LastDay'
}


# Hilfs-Routine (DEBUG)

def DEBUG(*s):
    # type: (object) -> object
    out = [datetime.datetime.now().isoformat() + ':', ] + [str(x) for x in s]
    print(' '.join(out))


def on_publish(client, userdata, result):  # create function for callback
    print("data published \n")
    pass


from solarmax_lib3 import SolarMax

DB_FILE = '/database.db'

inverters = {'192.168.178.6': [1, ],

             }

smlist = []
for host in inverters.keys():
    sm = SolarMax(host, 12345)
    sm.use_inverters(inverters[host])
    smlist.append(sm)

allinverters = []
for host in inverters.keys():
    allinverters.extend(inverters[host])

while True:
    pac_gesamt = 0.0
    daysum_gesamt = 0.0

    count = 0
    for sm in smlist:
        for (no, ivdata) in sm.inverters().items():
            try:
                (inverter, current) = sm.query(no, query_types)
                count += 1
            except Exception as e:
                # Communication error. Probably no sunshine so inverter is off
                DEBUG(e, ', Error while query for inverter: ', no)
                continue

            ivmax = ivdata['installed']
            ivname = ivdata['desc']
            PAC = current['IL1'] * current['UL1']
            percent = int((PAC / ivmax) * 100)
            PDC = current['IDC'] * current['UDC']
            (status, errors) = sm.status(no)
            if errors:
                print('WR %i: %s (%s)' % (no, status, errors))

            print('''
      WR %i (%s)
        Status: %s
        Aktuell: %9.1f Watt / errechnet: %8.1f W (%i %% von %i Watt)
        P_DC:    %9.1f Watt (Wirkungsgrad: %i %%)
        Gesamt heute:   %8.1f   kWh
        Gesamt bisher:  %8.1f   kWh (seit %s)
      ''' % (inverter, ivname, status, current['PAC'], PAC, percent, ivmax, PDC, int((float(PAC) / PDC) * 100),
             current['KDY'], current['KT0'], current['FDAT'].date())
                  )
            pac_gesamt += current['PAC']
            daysum_gesamt += current['KDY']
    try:
        # Publish MQTT Message
        client1 = paho.Client("control1")  # create client object
        client1.on_publish = on_publish  # assign function to callback
        client1.connect(broker, port)  # establish connection

        pubstring = '{'
        for query in query_types:
            pubstring = pubstring + '"' + query_codes[query] + '"' + ':' + str(current[query]) + ','
        pubstring = pubstring[:-1]  # removes last comma again
        pubstring = pubstring + '}'
        print(pubstring)
        ret = client1.publish('Solarmax/ENERGY', pubstring)  # publish
    except Exception as e:
        print(e)
        pass
    if count < len(allinverters):
        print('Too less inverter found (%i < %i)' % (count, len(allinverters)))

    time.sleep(10)
