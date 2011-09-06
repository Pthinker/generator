#!/usr/bin/python2.5
#-*- coding: utf-8 -*-

from db.db_conn import DBManager
import os
import sys
import simplejson
from shutil import copy2
from glob import glob
import traceback
from metric.logger.logger import MLogger
from metric.libs import is_int, log_traceback
from metric.file_man.jfiles import JFileValidator
from metric.conf import ConfigReader
from metric.report_validator import ReportValidator
import MySQLdb

import warnings
warnings.simplefilter("error", MySQLdb.Warning)


_sql_insert = "INSERT INTO %s (%s) VALUES (%s)"

#_table_info dic stores pk names, field names for updating and where to take value to update those fields
_tables_info = {
    'report_data_set_column': {'pk': 'report_data_set_column_id', 'updates': {'element_id': '<new_element_id>'}},
    'report_data_set_pivot': {'pk': 'report_data_set_pivot_id', 'updates': {
            'element_id': '<new_element_id>',
            'last_updated_by': '<last_updated_by>',
            'pivot_column_value_column_id': 'report_data_set_column',
            'pivot_column_sort_column_id': 'report_data_set_column',
            'pivot_row_value_column_id': 'report_data_set_column',
            'pivot_row_sort_column_id': 'report_data_set_column',
            'pivot_measure_column_id': 'report_data_set_column',
        }
    },
    'report_data_set_chart': {'pk': 'report_data_set_chart_id', 'updates': {
            'element_id': '<new_element_id>',
            'last_updated_by': '<last_updated_by>',
            'report_data_set_pivot_id': 'report_data_set_pivot',
            'chart_by_report_data_set_column_id': 'report_data_set_column',
            'sector_value_data_set_column_id': 'report_data_set_column'
        }
    },
    'report_data_set_chart_column': {'pk': 'report_data_set_chart_column_id', 'updates': {
            'report_data_set_chart_id': 'report_data_set_chart',
            'report_data_set_column_id': 'report_data_set_column'
        }
    },
    'report_data_set_chart_element_property': {'pk': 'report_data_set_chart_element_property_id', 'updates': {'report_data_set_chart_id': 'report_data_set_chart'}},
    'report_data_set_row_value': {'pk': 'report_data_set_row_value_id', 'fks': {
            'report_data_set_column_id': 'report_data_set_column'
        }
    },
    'report_data_set_chart_row_value': {'pk': 'report_data_set_chart_row_value_id', 'updates': {
            'last_updated_by': '<last_updated_by>',
            'report_data_set_chart_id': 'report_data_set_chart',
            'report_data_set_row_value_id': 'report_data_set_row_value'
        }
    },
    'metric_drill_to_report': {'pk': 'metric_drill_to_report_id', 'updates': {
            'last_updated_by': '<last_updated_by>',
            'report_element_id': '<new_element_id>'
        }
    }
}



#_select_info keeps SQL requests to retrieve data which has to be cloned
_select_info = {
    'report_data_set_column': "SELECT * FROM report_data_set_column WHERE element_id = %s",
    'report_data_set_pivot': "SELECT * FROM report_data_set_pivot WHERE element_id = %s",
    'report_data_set_chart': "SELECT * FROM report_data_set_chart WHERE element_id = %s",
    'report_data_set_chart_non_pivot': "SELECT * FROM report_data_set_chart WHERE element_id = %s AND (ISNULL(report_data_set_pivot_id) OR report_data_set_pivot_id=0)",

    'report_data_set_chart_column': """
    SELECT report_data_set_chart_column.*
    FROM report_data_set_chart_column, report_data_set_chart
    WHERE report_data_set_chart.report_data_set_chart_id =  report_data_set_chart_column.report_data_set_chart_id AND element_id = %s
    """,

    'report_data_set_chart_element_property': """
    SELECT report_data_set_chart_element_property.*
    FROM report_data_set_chart_element_property, report_data_set_chart
    WHERE report_data_set_chart.report_data_set_chart_id =  report_data_set_chart_element_property.report_data_set_chart_id AND element_id = %s
    """,

    'report_data_set_row_value': """
    SELECT report_data_set_row_value.*
    FROM report_data_set_row_value, report_data_set_column
    WHERE report_data_set_row_value.report_data_set_column_id =  report_data_set_column.report_data_set_column_id AND element_id = %s
    """,

    'report_data_set_chart_row_value': """
    SELECT report_data_set_chart_row_value.*
    FROM report_data_set_chart_row_value, report_data_set_chart
    WHERE report_data_set_chart.report_data_set_chart_id =  report_data_set_chart_row_value.report_data_set_chart_id AND element_id = %s
    """,

    'metric_drill_to_report': "SELECT * FROM metric_drill_to_report WHERE report_element_id = %s"
}

def clone(_db, original_element_id, new_element_id, clone_pivots=False, clone_charts=False, clone_drill_to_links=False):
    """
    Clones report
    """
    #try
    #_db = DBManager().get_query()

    # tables to clone
    tables = ['report_data_set_column']
    if clone_pivots:
        tables += ['report_data_set_pivot']
        if clone_charts:
            tables += ['report_data_set_chart', 'report_data_set_chart_column', 'report_data_set_chart_element_property', 'report_data_set_row_value', 'report_data_set_chart_row_value']
    elif clone_charts:
        tables += ['report_data_set_chart', 'report_data_set_chart_column', 'report_data_set_chart_element_property', 'report_data_set_row_value', 'report_data_set_chart_row_value']
        _select_info['report_data_set_chart'] = _select_info['report_data_set_chart_non_pivot']

    if clone_drill_to_links:
        tables += ['metric_drill_to_report']

    # collect values for updating
    update_values = {'<new_element_id>': new_element_id}
    _db.query("SELECT last_updated_by FROM dashboard_element WHERE element_id = %s", (original_element_id, ))
    update_values['<last_updated_by>'] = _db.record[0]['last_updated_by']

    # clone table data
    _clone_relation_data(_db, original_element_id, tables, _tables_info, _select_info, update_values)
    # clone files
    config = ConfigReader()
    original_saved_data_path = os.path.join(config.report_root, str(original_element_id), "validation", "saved_data")
    new_saved_data_path = os.path.join(config.report_root, str(new_element_id), "validation", "saved_data")
    if not os.path.exists(new_saved_data_path):
        os.makedirs(new_saved_data_path)
    src = os.path.join(original_saved_data_path, 'valid.json')
    dst = os.path.join(new_saved_data_path, 'valid.json')
    try:
        # clone valid.json, substitute element_id in it
        json_file = open(src, 'r')
        content = json_file.read()
        json_file.close()
        json = simplejson.loads(content)
        json['element_id'] = str(new_element_id)
        json_file = open(dst, 'w')
        json_file.write(simplejson.dumps(json))
        json_file.close()
        os.chmod(dst, 0777)
        os.chown(dst, config.file_owner_uid, config.file_owner_gid)

        # clone *.dmp files
        for dmp_file in glob(os.path.join(original_saved_data_path, "*.dmp")):
            filename = os.path.basename(dmp_file)
            dst = os.path.join(new_saved_data_path, filename)
            copy2(dmp_file, dst)
            os.chmod(dst, 0777)
            os.chown(dst, config.file_owner_uid, config.file_owner_gid)
    except IOError, e:
        raise Exception("Cannot copy file into %s. %s" % (dst, e))
    except OSError, e:
        raise Exception("Cannot copy file into  %s. %s" % (dst, e))

    # Restore validation files from saved_data and run validation to create all required files.
    jfile = JFileValidator(config.report_root, new_element_id, None)
    jfile.restore_validation_data()
    reportValidator = ReportValidator()
    reportValidator.init(new_element_id)
    reportValidator.report_generation()

#        print simplejson.dumps({'status': 'OK', 'message': 'report %s duplicated to %s successfully' % (original_element_id, new_element_id)})
#    except Exception, message:
#        _logger.error("Report duplication. Report %s duplication failed. Exception %s" % (original_element_id, message))
#        log_traceback(_logger, traceback)
#        print simplejson.dumps({'status':'ERROR', 'message':'report %s duplication failed. %s' % (original_element_id, message)})


def _clone_relation_data(_db, object_id, tables, table_info, select_info, update_values):
    """
    Clone relation data.
    @param object_id: root record id, for example element_id
    @param tables: list of tables to copy (order is respected)
    @param table_info: dict with table infos: pk (name of the pk) and updates (list of columns to update and where to take value)
    @param select_info: SQL for each table to retrieve data to clone
    @param update_values: these values are to update fields in "updated" list
    """
    # Is used to store old ids/new ids as key/value pairs for each table
    id_mapping = {}
    # Iterate over all tables
    for table_name in tables:
        table = table_info[table_name]
        id_mapping[table_name] = {}
        _db.Query(select_info[table_name], (object_id, ))
        for record in _db.record:
            original_id = record[table['pk']]
            # remove pk
            del record[table['pk']]
            # update fks
            for fk_name, fk_value in table['updates'].items():
                if fk_value[0] == '<':
                    record[fk_name] = update_values[fk_value]
                else:
                    if record[fk_name] is not None:
                        record[fk_name] = id_mapping[fk_value][record[fk_name]]
            column_count = len(record)
            columns_sql = ", ".join(record.keys())
            values_placeholders_sql = '%s, ' * column_count
            sql_insert = _sql_insert % (table_name, columns_sql, values_placeholders_sql[:-2])

            _db.Query(sql_insert, tuple(record.values()))
            id_mapping[table_name][original_id] = db.lastInsertID

def _clone_element(_db, original_element_id, new_element_id):
    """
    Used to clone dashboard element. Only in test mode (-t), because in real usage element is cloned by php backend
    """
    _db.Query("SELECT * FROM dashboard_element WHERE element_id = %s", (original_element_id, ))
    record = dict(_db.record[0])
    record['element_id'] = new_element_id
    #record['primary_topic_id'] = None
    column_count = len(record)
    columns_sql = ", ".join(record.keys())
    values_placeholders_sql = "%s, " * column_count
    sql_insert = _sql_insert % ('dashboard_element', columns_sql, values_placeholders_sql[:-2])
    _db.Query(sql_insert, tuple(record.values()))

if __name__ == "__main__":
    logger = MLogger('report duplication')
    _logger = logger.get_logger()

    params = sys.argv[1:]
    usage = """Usage:
report_duplicate.py <original_element_id> <new_element_id> [-t] [-p[-c]] [-d] | -h

<original_element_id> : original dashboard element_id.
<new_element_id>      : new dashboard element_id.
-p                    : duplicate pivots
-c                    : duplicate charts. Duplicate all charts if -p is set, only non-pivot charts if -p not set.
-d                    : duplicate drill-to links.
-t                    : only in the test mode, element is duplicated (in real usage it's done by the php backend)
-h,--h,-help,--help   : show this message.

Notice:
This module supposes that duplication of the dashboard_element record is done by UI. With option "-t" it can clone record,
but it's only to test without UI

Examples:
1) Duplicate element 2 to element 500 without pivots, charts and drill-to-detail:
sudo report_duplicate.py 2 500
2) Duplicate element 2 to element 500 with pivots and drill-to-detail:
sudo report_duplicate.py 2 500 -p -d
2) Duplicate element 2 to element 500 with all data (pivots, charts, drill-to-detail)
sudo report_duplicate.py 2 500 -p -c -d

"""
    if params:
        _logger.info('Duplicator run: %s' % ' '.join(sys.argv))
        if params[0] == '-h' or params[0] == '--h' or params[0] == '-help' or params[0] == '--help':
            print usage
            sys.exit()
        if is_int(params[0]) and is_int(params[1]):
            orig_element_id = int(params[0])
            new_element_id = int(params[1])


#            try:
#                from metric.report_validator import ReportValidator
#                from db.db_conn import DBManager
#            except Exception, e:
#                if str(e).find('Check file path and permissions'):
#                    print "This module has to be run under the root permissions."
#                    sys.exit()
#                else:
#                    raise
            try:
                db = DBManager().get_query()
                clone_pivots = '-p' in params
                #clone_charts = clone_pivots and '-c' in params
                clone_charts = '-c' in params
                clone_drill_to_links = '-d' in params
                if '-t' in params:
                    _clone_element(db, orig_element_id, new_element_id)
#                    try:
#                        _clone_element(int(params[0]), int(params[1]))
#                    except Exception, e:
#                        # pass warning from MySQL
#                        if str(e) == 'No data - zero rows fetched, selected, or processed':
#                            pass
#                        else:
#                            raise
                clone(db, orig_element_id, new_element_id, clone_pivots, clone_charts, clone_drill_to_links)
            except MySQLdb.Error, message:
                error_message = "Report duplicator. Coping report %s to %s failed. SQL error %s" % (orig_element_id, new_element_id, message)
                _logger.error(error_message)
                log_traceback(_logger, traceback)
                print simplejson.dumps({'status':'ERROR', 'message': error_message})
                exit()
            except Exception, exc:
                error_message = "Report duplicator. Coping report %s to %s failed. Exception %s" % (orig_element_id, new_element_id, exc)
                _logger.error(error_message)
                log_traceback(_logger, traceback)
                print simplejson.dumps({'status':'ERROR', 'message': error_message})
                exit()
            print simplejson.dumps({'status':'OK', 'message': ''})
        else:
            print usage
            sys.exit()
    else:
        print usage
