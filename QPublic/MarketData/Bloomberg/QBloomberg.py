from __future__ import print_function
import blpapi
from collections import defaultdict
from pandas import DataFrame
from datetime import datetime, date, time
import pandas as pd
import numpy as np
import sys
from pprint import pprint
import warnings
import six
from dateutil.relativedelta import relativedelta

class QBBGClient():
    def __init__(self, host=None, port=8194, appName=None):

        self.Config=None
        host = host or 'localhost'
        port = port or 8194
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost(host)
        sessionOptions.setServerPort(port)

        if (host.lower() != 'localhost') and (host !='127.0.0.1'):
            sessionOptions.setAuthenticationOptions("AuthenticationMode=APPLICATION_ONLY;ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=JPSRV")

        self.initialized_services = set()
        self.session = blpapi.Session(sessionOptions)
        if not self.session.start():
            print("Failed to start session")

        self.session.nextEvent()

    def getSAPIConfigs(self):
        return self.Config

    def service_refData(self):

        if '//blp/refdata' in self.initialized_services:
            return

        if not self.session.openService("//blp/refdata"):
            print("Failed to open //blp/refdata")

        self.session.nextEvent()
        self.refDataService = self.session.getService("//blp/refdata")
        self.session.nextEvent()
        self.initialized_services.add('//blp/refdata')

    def bdh(self, ticker_list, fld_list, start_date, end_date=date.today().strftime('%Y%m%d'), periodselection='DAILY',
            overrides=None, other_request_parameters=None, move_dates_to_period_end=False, adjustmentNormal=True,adjustmentAbnormal=True,adjustmentSplit=True):

        self.service_refData()

        if isstring(ticker_list):
            ticker_list = [ticker_list]
        if isstring(fld_list):
            fld_list = [fld_list]

        if hasattr(start_date, 'strftime'):
            start_date = start_date.strftime('%Y%m%d')
        if hasattr(end_date, 'strftime'):
            end_date = end_date.strftime('%Y%m%d')

        request = self.refDataService.createRequest("HistoricalDataRequest")
        for t in ticker_list:
            request.getElement("securities").appendValue(t)
        for f in fld_list:
            request.getElement("fields").appendValue(f)

        request.set("periodicitySelection", periodselection)
        request.set("startDate", start_date)
        request.set("endDate", end_date)

        request.set("adjustmentNormal", adjustmentNormal);
        request.set("adjustmentAbnormal", adjustmentAbnormal);
        request.set("adjustmentSplit", adjustmentSplit);

        if overrides is not None:
            overrideOuter = request.getElement('overrides')
            for k in overrides:
                override1 = overrideOuter.appendElement()
                override1.setElement('fieldId', k)
                override1.setElement('value', overrides[k])

        if other_request_parameters is not None:
            for k, v in six.iteritems(other_request_parameters):
                request.set(k, v)

        def adjust_date(to_adjust):
            if periodselection == 'MONTHLY':
                # always make the date the last day of the month
                return date(to_adjust.year, to_adjust.month, 1) + relativedelta(months=1) - relativedelta(days=1)
            if periodselection == 'WEEKLY':
                return to_adjust + relativedelta(weekday=4)

            return to_adjust

        self.session.sendRequest(request)
        data = defaultdict(dict)
        while (True):
            ev = self.session.nextEvent(500)
            for msg in ev:
                ticker = msg.getElement('securityData').getElement('security').getValue()
                fieldData = msg.getElement('securityData').getElement('fieldData')
                for i in range(fieldData.numValues()):
                    for j in range(1, fieldData.getValue(i).numElements()):
                        dt = fieldData.getValue(i).getElement(0).getValue()
                        if move_dates_to_period_end:
                            dt = adjust_date(dt)

                        data[(ticker, fld_list[j - 1])][dt] = fieldData.getValue(i).getElement(j).getValue()

            if ev.eventType() == blpapi.Event.RESPONSE:
                break

        if len(fld_list) == 1:
            data = {k[0]: v for k, v in data.items()}
            data = DataFrame(data)
            data = data[ticker_list]
            data.index = pd.to_datetime(data.index)
            return data

        if len(data) == 0:
            return DataFrame()

        data = DataFrame(data)
        data = data[ticker_list]
        data.columns = pd.MultiIndex.from_tuples(data, names=['ticker', 'field'])
        data.index = pd.to_datetime(data.index)
        return data

    def bdib(self, ticker, fld_list, startDateTime, endDateTime, eventType='TRADE', interval=1):
        """
        Get one ticker (Only one ticker available per call); eventType (TRADE, BID, ASK,..etc); interval (in minutes)
                ; fld_list (Only [open, high, low, close, volumne, numEvents] availalbe)
        return pandas dataframe with return Data
        """
        self.service_refData()
        request = self.refDataService.createRequest("IntradayBarRequest")
        request.set("security", ticker)
        request.set("eventType", eventType)
        request.set("interval", interval)  # bar interval in minutes
        request.set("startDateTime", startDateTime)
        request.set("endDateTime", endDateTime)

        self.session.sendRequest(request)
        data = defaultdict(dict)
        while (True):
            ev = self.session.nextEvent(500)
            for msg in ev:
                barTickData = msg.getElement('barData').getElement('barTickData')
                for i in range(barTickData.numValues()):
                    for j in range(len(fld_list)):
                        data[(fld_list[j])][barTickData.getValue(i).getElement(0).getValue()] = barTickData.getValue(
                            i).getElement(fld_list[j]).getValue()

            if ev.eventType() == blpapi.Event.RESPONSE:
                break

        data = DataFrame(data)
        data.index = pd.to_datetime(data.index)
        return data

    def bdp(self, ticker, fld_list, overrides=None):
        self.service_refData()

        request = self.refDataService.createRequest("ReferenceDataRequest")
        if isstring(ticker):
            ticker = [ticker]

        securities = request.getElement("securities")
        for t in ticker:
            securities.appendValue(t)

        if isstring(fld_list):
            fld_list = [fld_list]

        fields = request.getElement("fields")
        for f in fld_list:
            fields.appendValue(f)

        if overrides is not None:
            overrideOuter = request.getElement('overrides')
            for k in overrides:
                override1 = overrideOuter.appendElement()
                override1.setElement('fieldId', k)
                override1.setElement('value', overrides[k])

        self.session.sendRequest(request)
        data = dict()

        while (True):
            ev = self.session.nextEvent(500)
            for msg in ev:
                securityData = msg.getElement("securityData")

                for i in range(securityData.numValues()):
                    fieldData = securityData.getValue(i).getElement("fieldData")
                    secId = securityData.getValue(i).getElement("security").getValue()
                    data[secId] = dict()
                    for field in fld_list:
                        if fieldData.hasElement(field):
                            data[secId][field] = fieldData.getElement(field).getValue()
                        else:
                            data[secId][field] = np.NaN

            if ev.eventType() == blpapi.Event.RESPONSE:
                break

        return pd.DataFrame.from_dict(data)

    def bds(self, security, field, overrides=None):

        self.service_refData()

        request = self.refDataService.createRequest("ReferenceDataRequest")
        assert isstring(security)
        assert isstring(field)

        securities = request.getElement("securities")
        securities.appendValue(security)

        fields = request.getElement("fields")
        fields.appendValue(field)

        if overrides is not None:
            overrideOuter = request.getElement('overrides')
            for k in overrides:
                override1 = overrideOuter.appendElement()
                override1.setElement('fieldId', k)
                override1.setElement('value', overrides[k])

        self.session.sendRequest(request)
        data = dict()

        while (True):
            ev = self.session.nextEvent(500)
            for msg in ev:
                securityData = msg.getElement("securityData")
                for i in range(securityData.numValues()):
                    fieldData = securityData.getValue(i).getElement("fieldData").getElement(field)
                    for i, row in enumerate(fieldData.values()):
                        for j in range(row.numElements()):
                            e = row.getElement(j)
                            k = str(e.name())
                            v = e.getValue()
                            if k not in data:
                                data[k] = list()

                            data[k].append(v)

            if ev.eventType() == blpapi.Event.RESPONSE:
                break

        return pd.DataFrame.from_dict(data)

    def stop(self):
        self.session.stop()

def isstring(s):
    return isinstance(s, str)

def processMessage(msg):
    SECURITY_DATA = blpapi.Name("securityData")
    SECURITY = blpapi.Name("security")
    FIELD_DATA = blpapi.Name("fieldData")
    FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
    FIELD_ID = blpapi.Name("fieldId")
    ERROR_INFO = blpapi.Name("errorInfo")

    securityDataArray = msg.getElement(SECURITY_DATA)
    for securityData in securityDataArray.values():
        print(securityData.getElementAsString(SECURITY))
        fieldData = securityData.getElement(FIELD_DATA)
        for field in fieldData.elements():
            for i, row in enumerate(field.values()):
                for j in range(row.numElements()):
                    e = row.getElement(j)
                    print("Row %d col %d: %s %s" % (i, j, e.name(), e.getValue()))