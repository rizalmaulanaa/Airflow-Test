import random
import mysql.connector

from datetime import datetime

def mysql_connect(database):
    connection = mysql.connector.connect(host='localhost',
                                        database=database,
                                        user='root',
                                        password='XXXXXXX')
    if connection.is_connected():
        return connection  
    else:
        return "Connection Error"   


def set_data_sales(database, **kwargs):
    conn = mysql_connect(database)
    mycursor = conn.cursor()

    date_exe = kwargs['logical_date']
    date_exe_start = date_exe.replace(hour=00, minute=00, second=00)
    date_exe_start = date_exe_start.strftime('%Y-%m-%d %X')
    date_exe_end = date_exe.replace(hour=23, minute=59, second=00)
    date_exe_end = date_exe_end.strftime('%Y-%m-%d %X')

    sql_select = """
    select inline.InvoiceId, cust.CustomerId, Album.AlbumId, Artist.ArtistID, invoice.InvoiceDate, inline.UnitPrice*inline.Quantity as Total
    from InvoiceLine as inline
    inner join Invoice on inline.InvoiceId=Invoice.InvoiceId
    inner join Customer as cust on Invoice.CustomerId=cust.CustomerId
    inner join Track on inline.TrackId=Track.TrackId
    inner join Album on Track.AlbumId=Album.AlbumId
    inner join Artist on Album.ArtistId=Artist.ArtistID
    where invoice.InvoiceDate between '{}' AND '{}';"""
    sql_select = sql_select.format(date_exe_start, date_exe_end)
    mycursor.execute(sql_select)
    myresult = mycursor.fetchall()

    all_data = []
    for idx in myresult:
        idx = list(idx)
        idx[-2] = idx[-2].strftime('%Y-%m-%d %X')
        all_data.append(tuple(idx))

    sql_insert = "insert into sales (InvoiceId, CustomerId, AlbumId, ArtistID, InvoiceDate, Total) values (%s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sql_insert, all_data)
    conn.commit()

    conn.close()

def set_data_employees(database, **kwargs):
    conn = mysql_connect(database)
    mycursor = conn.cursor()

    date_exe = kwargs['logical_date']
    date_exe_start = date_exe.replace(hour=00, minute=00, second=00)
    date_exe_start = date_exe_start.strftime('%Y-%m-%d %X')
    date_exe_end = date_exe.replace(hour=23, minute=59, second=00)
    date_exe_end = date_exe_end.strftime('%Y-%m-%d %X')

    sql_select = """
    select emp.employee_id, emp.hire_date, emp.salary, dpt.department_id, loc.location_id, coun.country_id, reg.region_id
    from employees as emp 
    left join departments as dpt on emp.department_id=dpt.department_id
    inner join locations as loc on dpt.location_id=loc.location_id
    inner join countries as coun on loc.country_id=coun.country_id
    inner join regions as reg on coun.region_id=reg.region_id;
    where  emp.hire_date between '{}' AND '{}';"""
    sql_select = sql_select.format(date_exe_start, date_exe_end)
    mycursor.execute(sql_select)
    myresult = mycursor.fetchall()

    all_data = []
    for idx in myresult:
        idx = list(idx)
        idx[1] = idx[1].strftime('%Y-%m-%d %X')
        all_data.append(tuple(idx))

    sql_insert = "insert into track_employees (employee_id, hire_date, salary, department_id, location_id, country_id, region_id) values (%s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sql_insert, all_data)
    conn.commit()

    conn.close()