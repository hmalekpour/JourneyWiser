from flask import Flask
from flask import request
import psycopg2
import datetime as dt

app = Flask(__name__)

def query(list_id, date):
    
    #constants used to establish connection to postgres database
    DB_NAME = "postgres" 
    USER_NAME = "postgres"
    HOST = "ec2-18-191-205-97.us-east-2.compute.amazonaws.com"
    PORT = 5432

    #DB connection
    conn = psycopg2.connect(database = DB_NAME, user = USER_NAME, host = HOST, port = PORT)
    cursor = conn.cursor()

    # get the lead time data for the given date
    cursor.execute("""SELECT "min(lead_time)" FROM lead_time_prediction WHERE listing_id = %(listing_id)s AND date = %(date)s""", {"listing_id": list_id, "date": dt.datetime.strptime(date,"%Y-%m-%d").date()})
    
    #fetch the data
    rows = cursor.fetchall()

    #close DV connection
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
    rows = query(req_id, req_date)
    if len(rows) == 0:
        return "No data availabe!"
    else:
        ret = rows[0][0]
        if ret == 999:
            return "Fully Booked!"
        else:
            return "Lead time is {} days.".format(ret) 
    
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80)
