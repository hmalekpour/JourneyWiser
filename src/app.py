from flask import Flask
from flask import request
import psycopg2
import datetime as dt

app = Flask(__name__)

def query(list_id, date):

    #list_id = 958
    #date = '2020-02-19'

    #DB connection
    conn = psycopg2.connect(database="postgres", user="postgres", host="ec2-18-191-205-97.us-east-2.compute.amazonaws.com", port=5432)

    # create a psycopg2 cursor that can execute queries
    cursor = conn.cursor()

    # find the primary key of listing table for the given listing_id
    #cursor.execute("SELECT id FROM listing WHERE listing_id = %s" % list_id)
    #list_table_id = cursor.fetchall()[0][0]

    # get the lead time data for the given date

    cursor.execute("""SELECT "min(lead_time)" FROM lead_time_prediction WHERE listing_id = %(listing_id)s AND date = %(date)s""", {"listing_id": list_id, "date": dt.datetime.strptime(date,"%Y-%m-%d").date()})
    rows = cursor.fetchall()
    
    #for row in rows:
    #    print(row)

    cursor.close()
    conn.close()
    return rows

@app.route('/')
def hello():
    print("Incoming request")
    return "Hello!"

@app.route('/check-listing')
def check_listing():
    req_id = request.args.get('id')
    req_date = request.args.get('date')
    # your logic to get the result
    rows = query(req_id, req_date)
    if len(rows) == 0:
        return "No data availabe!"
    else:
        ret = rows[0][0]
        if ret == 999:
            return "Fully Booked!"
        else:
            return "Lead time is {} days.".format(ret) 
    #"Response from AWS = " + str(rows) + ". Checking availability for listing with id = " + req_id + " and date = " + req_date

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80)
