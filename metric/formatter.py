#-*- coding: utf-8 -*-
from db.db_conn import DBManager
from decimal import Decimal
import datetime
from simplejson.ordered_dict import OrderedDict
import sys

"""
this is a magic transformation from mysql date format rule to python
"""
#%a     Abbreviated weekday name (Sun..Sat) - ok
#%b     Abbreviated month name (Jan..Dec) - ok
#%c     Month, numeric (0..12) - replace with "%-m"
#%D     Day of the month with English suffix (0th, 1st, 2nd, 3rd, )
#%d     Day of the month, numeric (00..31)
#%e     Day of the month, numeric (0..31) - replace with "%-d"
#%f     Microseconds (000000..999999) - ??? replace on generation by substituting real microseconds  
#%H     Hour (00..23) - ok
#%h     Hour (01..12) - replace with "%I"
#%I     Hour (01..12) - ok
#%i     Minutes, numeric (00..59) - replace with "%M"
#%j     Day of year (001..366) - ok
#%k     Hour (0..23) - ok
#%l     Hour (1..12) - ok
#%M     Month name (January..December) - replace with "%B"
#%m     Month, numeric (00..12) - ok
#%p     AM or PM - ok
#%r     Time, 12-hour (hh:mm:ss followed by AM or PM) - ok
#%S     Seconds (00..59) - ok
#%s     Seconds (00..59) - replace with "%S"
#%T     Time, 24-hour (hh:mm:ss) - ok
#%U     Week (00..53), where Sunday is the first day of the week - ok
#%u     Week (00..53), where Monday is the first day of the week - replace with "%W"
#%V     Week (01..53), where Sunday is the first day of the week; used with %X - ok ??
#%v     Week (01..53), where Monday is the first day of the week; used with %x - ??? replace with "%W"
#%W     Weekday name (Sunday..Saturday) - replace with "%A"
#%w     Day of the week (0=Sunday..6=Saturday) - ok
#%X     Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V - ??? - replace with "%Y"
#%x     Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v - ??? - replace with "%Y"
#%Y     Year, numeric, four digits - ok
#%y     Year, numeric (two digits) - ok

date_format_replacement = OrderedDict()
date_format_replacement["%c"] = "%-m"
date_format_replacement["%e"] = "%-d"
date_format_replacement["%h"] = "%I"
date_format_replacement["%M"] = "%B"
date_format_replacement["%i"] = "%M"
date_format_replacement["%s"] = "%S"
date_format_replacement["%W"] = "%A"
date_format_replacement["%u"] = "%W"
date_format_replacement["%v"] = "%W"
date_format_replacement["%X"] = "%Y"
date_format_replacement["%x"] = "%Y"
date_format_replacement["%s"] = "%S"

class DateUnformat:
    date_format = ''

    def __init__(self, date_format = None):
        # convert mysql format to python format
        if date_format:
            for k, v in date_format_replacement.iteritems():
                date_format = date_format.replace(k, v) 
        self.date_format = date_format

    def unformat(self, var):
        date = None
        if var:
            if self.date_format:
                try:
                    date = datetime.datetime.strptime(var, self.date_format)
                except TypeError:
                    pass
                except ValueError:
                    date = self._unformat_by_default_mask(var)
            else:
                date = self._unformat_by_default_mask(var)
        return date

    def _unformat_by_default_mask(self, var):
        date = None
        try:
            date = datetime.datetime.strptime(var, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                date_without_time = datetime.datetime.strptime(var, '%Y-%m-%d')
                date = datetime.datetime.combine(date_without_time, datetime.time.min)
            except TypeError:
                pass
            except ValueError:
                # cannot format it
                pass
        return date

    def try_unformat(self, var):
        date = self.unformat(var)
        if date is None:
            #print 'tried to unformat as date but failed'
            if isinstance(var, str):
                date = unicode(var)
            else:
                date = var
        return date


class FieldFormatter:
    _inited = False
    _display_formats = {}
    _def_int_display_mask_id = 0
    _def_float_display_mask_id = 0
    _def_date_display_mask_id = 0
    _custom_date_display_mask = {}
    _custom_format_ind = False
    def __init__(self, def_date_format_id):
        self._def_date_display_mask_id = def_date_format_id
        self._db = DBManager.get_query()
        if not FieldFormatter._inited:
            FieldFormatter._inited = True
            
            #get all masks
            self._db.Query("""SELECT * FROM display_mask""")
            for format in self._db.record:
                if not format['display_precision_digits']:
                    format['display_precision_digits'] = 0
                if not format['prefix']:
                    format['prefix'] = ''
                if not format['suffix']:
                    format['suffix'] = ''
                if not format['thousands_delimiter']:
                    format['thousands_delimiter'] = ''
                if not format['decimal_point_delimiter']:
                    format['decimal_point_delimiter'] = '.'
                if format['mask_type'] == u'date':
                    format['date_format_string'] = format['date_format_string'].encode('ascii')
                    # convert mysql format to python format
                    for k, v in date_format_replacement.iteritems():
                        format['date_format_string'] = format['date_format_string'].replace(k, v)

                FieldFormatter._display_formats[format['display_mask_id']] = format
            
            # get default int mask id
            res = self._db.Query("""SELECT display_mask_id
                                                    FROM display_mask
                                                WHERE
                                                    default_int_mask_ind='Y'""")
            if res:
                format = self._db.record[0]
                self._def_int_display_mask_id = format['display_mask_id']

            # get default float mask id
            res = self._db.Query("""SELECT display_mask_id
                                                    FROM display_mask
                                                WHERE
                                                    default_float_mask_ind='Y'""")
            if res:
                format = self._db.record[0]
                FieldFormatter._def_float_display_mask_id = format['display_mask_id']
    
    def set_custom_date_format_rule(self, date_format_rule):
        if date_format_rule:
            custom_date_format = date_format_rule

            custom_date_format = custom_date_format.encode('ascii')
            # convert mysql format to python format
            for k, v in date_format_replacement.iteritems():
                custom_date_format = custom_date_format.replace(k, v)
            
            self._custom_date_display_mask = {
                'date_format_string': custom_date_format,
                'prefix': '',
                'suffix': '',
                'show_calendar_quarter_ind': ''
            }

            self._custom_format_ind = True
    
    def set_def_date_format_id(self, def_date_format_id):
        self._def_date_display_mask_id = def_date_format_id

    def format_date(self, date_val, display_mask_id=0):
        date = None
        if not date_val:
            return ''
        # check is it a datetime variable
        if not (isinstance(date_val, datetime.datetime) or isinstance(date_val, datetime.date)):
            # may be it's string
            if isinstance(date_val, str):
                # is it datetime
                try:
                    date = datetime.datetime.strptime(date_val, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    # is it date
                    try:
                        date = datetime.datetime.strptime(date_val, '%Y-%m-%d')
                    except ValueError:
                        # cannot format it
                        return date_val
        
        if isinstance(date_val, datetime.date) and not isinstance(date_val, datetime.datetime):
            date = datetime.datetime.combine(date_val, datetime.time(0, 0, 0))
        elif isinstance(date_val, datetime.datetime):
            date = date_val
        
        if self._custom_format_ind:
            display_mask = self._custom_date_display_mask
        else:
            if display_mask_id:
                display_mask = FieldFormatter._display_formats.get(display_mask_id, None)
            else:
                display_mask = FieldFormatter._display_formats.get(self._def_date_display_mask_id, None)
    
            if not display_mask:
                # cannot format it - no display mask
                return unicode(date)
    
            if display_mask['mask_type'] != 'date':
                # cannot format it - dsplay mask is not of date type
                return unicode(date)
        date_format_string = display_mask['date_format_string']
        
        #emulate quarter %Q
        quarter_letter = ''
        if display_mask['show_calendar_quarter_ind'] == 'Y' or date_format_string.find('%Q') != -1:
            res = self._db.Query("""SELECT short_name
                                        FROM calendar_quarter
                                    WHERE
                                        `first_day_of_quarter` <= %s
                                        AND `last_day_of_quarter` >= %s
                                    LIMIT 0, 1""", (date, date))
            if res:
                quarter_rec = self._db.record[0]
                quarter = quarter_rec['short_name'].encode('ascii')
            else:
                quarter = 'Q' + str((date.month-1)//3 + 1)

            date_format_string = date_format_string.replace("%Q", quarter)            
            
            if display_mask['show_calendar_quarter_ind'] == 'Y':
                quarter_letter = quarter

        #emulate mysql %f -  microsecond
        microsecond = date.microsecond
        date_format_string = date_format_string.replace("%f", str(microsecond).rjust(6, '0'))

        #emulate mysql %D - day num (1-31) with suffix
        day = date.day
        if day == 1:
            day_with_suffix = "%s%s"%(day, 'st')
        elif day == 2:
            day_with_suffix = "%s%s"%(day, 'nd')
        elif day == 2:
            day_with_suffix = "%s%s"%(day, 'rd')
        else:
            day_with_suffix = "%s%s"%(day, 'th')
        date_format_string = date_format_string.replace("%D", day_with_suffix)

        return "%s%s%s%s" %(display_mask['prefix'], quarter_letter, date.strftime(date_format_string), display_mask['suffix'])

    def detect_type(self, display_mask_id):
        """
        detect type by display_mask_id
        """
        value_type = 'text'
        if display_mask_id:
            display_mask = self._get_format(display_mask_id)
            value_type = 'text'
            if display_mask:
                value_type = display_mask['mask_type']
                if value_type == 'numeric':
                    value_type = 'numeric'
                elif value_type == 'date':
                    value_type = 'datetime'
        return value_type

    def detect_display_mask_id(self, column_index, data):
        """
        detect type of specified item of list of dicts. take first not empty element and detect its mask
        """
        field = None
        display_mask_id = 0
        for raw in data:
            if raw[column_index]:
                field = raw[column_index]
                break

        if field:
            display_mask_id = self.get_field_type(field)
        return display_mask_id

    def get_default_display_mask_id(self, value_type, numeric_type):
        if value_type == 'datetime':
            return self._def_date_display_mask_id
        elif value_type == 'numeric':
            if numeric_type == 'float':
                return self._def_float_display_mask_id
            else:
                return self._def_int_display_mask_id
        return 0

    def get_field_type(self, value):
        """
        detect type of value
        """
        if value:
            if isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
                #this is date value
                return self._def_date_display_mask_id

            if isinstance(value, unicode) or isinstance(value, str):
                try:
                    #this is date value
                    datetime.datetime.strptime(value, "%Y-%m-%d")
                    return self._def_date_display_mask_id
                except ValueError:
                    pass

            if isinstance(value, float) or isinstance(value, complex):
                #this is float value
                return self._def_float_display_mask_id  

            if isinstance(value, int) or isinstance(value, long):
                #this is int value
                return self._def_int_display_mask_id

            if isinstance(value, Decimal):
                if str(value) == str(Decimal(long(value))):
                    #this is decimal value and probably int
                    return self._def_int_display_mask_id
                else:
                    #this is decimal value and probably float
                    return self._def_float_display_mask_id
        # can't determine. probably this is string value
        return 0

                
    def _get_format(self, display_mask_id):
        """
        returns existing format or fetch it from db if it's new
        """
        if display_mask_id in FieldFormatter._display_formats:
            return FieldFormatter._display_formats[display_mask_id]
        return None
    
    
    def format_full(self, value, display_mask_id):
        """
        returns fully formatted value
        """
        if display_mask_id:
            display_mask = FieldFormatter._display_formats.get(display_mask_id, None)
        else:
            display_mask = None
        return self._raw_format(value, display_mask, full = True)

    def format_full_custom(self, value, display_mask):
        """
        returns fully formatted value
        """
        return self._raw_format(value, display_mask, full = True)

    def pre_format(self, value, display_mask_id):
        """
        returns pre formatted value
        """
        return self._raw_format_orig(value, display_mask_id)
    
    def reduce_format(self, values, display_mask_id):
        display_mask = FieldFormatter._display_formats.get(display_mask_id, None)
        reduced_values = []
        if display_mask and display_mask['mask_type'] == 'numeric':
            if display_mask['show_amount_as'] == 'Thousands':
                reduced_values = [value / 1000.0 for value in values] 
            elif display_mask['show_amount_as'] == 'Millions':
                reduced_values = [value / 1000000.0 for value in values]
            else:
                reduced_values = values
        return reduced_values

    def _raw_format(self, value, display_mask, full):
        if value is None:
            return ''

        if display_mask:
            #this is date
            if display_mask['mask_type'] == 'date':
                #return self.format_date(value, display_mask_id)
                return self.format_date(value, display_mask['display_mask_id'])

            if not isinstance(value, str):
                if isinstance(value, Decimal):
                    if str(value) == str(Decimal(long(value))):
                        value = long(value)
                    else:
                        value = float(value)
                if full:
                    value = self._reduce(value, display_mask)
                value = round(value, display_mask['display_precision_digits'])
                format_string = "%." + str(display_mask['display_precision_digits']) + "f"
                formatted_value = format_string % value
                formatted_value = self._split_thousands(formatted_value, str(display_mask['thousands_delimiter']), str(display_mask['decimal_point_delimiter']))
                return "%s%s%s" % (display_mask['prefix'], formatted_value, display_mask['suffix'])

        if not isinstance(value, unicode):
            return unicode(value)

        return value

    def _raw_format_orig(self, value, display_mask_id):
        #, reduce
        if value is None:
            return value
        if display_mask_id:
            display_mask = FieldFormatter._display_formats.get(display_mask_id, None)
            if display_mask:
                #this is date
                if display_mask['mask_type'] == 'date':
                    return value

                if display_mask['mask_type'] == 'numeric':
                    if isinstance(value, Decimal):
                        if str(value) == str(Decimal(long(value))):
                            value = long(value)
                        else:
                            value = float(value)
                    #value = round(value, display_mask['display_precision_digits'])
                    #if not display_mask['display_precision_digits']:
                    #    value = long(value)
                    return value
        return value

    def _reduce(self, value, display_mask):
        if display_mask['show_amount_as'] == 'Thousands':
            value /= 1000.0
        elif display_mask['show_amount_as'] == 'Millions':
            value /= 1000000.0
        return value
    
    def _split_thousands(self, s, tSep, dSep="."):
        if dSep != "" and s.rfind('.') > 0:
            rhs = s[s.rfind('.') + 1:]
            s = s[:s.rfind('.')]
            if len(s) <= 3 or (len(s) == 4 and s[0] == '-'): return s + dSep + rhs
            return self._split_thousands(s[:-3], tSep) + tSep + s[-3:] + dSep + rhs
        else:
            if len(s) <= 3 or (len(s) == 4 and s[0] == '-'): return s
            return self._split_thousands(s[:-3], tSep) + tSep + s[-3:]
