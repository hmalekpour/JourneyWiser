from flask import Flask
from flask import request
import psycopg2
import datetime as dt
from datetime import timedelta

app = Flask(__name__)

def find_leadtime(list_id, date):
    
    #constants used to establish connection to postgres database
    DB_NAME = "postgres" 
    USER_NAME = "postgres"
    HOST = "ec2-18-223-205-169.us-east-2.compute.amazonaws.com"
    PORT = 5432

    #DB connection
    conn = psycopg2.connect(database = DB_NAME, user = USER_NAME, host = HOST, port = PORT)
    cursor = conn.cursor()

    # get the lead time data for the given date
    cursor.execute("""SELECT "min(lead_time)" FROM leadtime_history WHERE listing_id = %(listing_id)s AND date = %(date)s"""\ 
                    , {"listing_id": list_id, "date": (dt.datetime.strptime(date,"%Y-%m-%d")-timedelta(days=364)).date()})
    r1 = cursor.fetchall()
    
    #get the average lead time for selected month
    cursor.execute("""SELECT AVG("min(lead_time)") FROM leadtime_history WHERE listing_id = %(listing_id)s AND EXTRACT(MONTH FROM date) = %(month)s AND "min(lead_time)" != 999 AND "min(lead_time)" < 120 """\
                    , {"listing_id": list_id, "month": dt.datetime.strptime(date,"%Y-%m-%d").month})
    r2 = cursor.fetchall()

    #close DB connection
    cursor.close()
    conn.close()
    results=[r1,r2]
    return results

@app.route('/')
def hello():
    print("Incoming request")
    return "Hello!"

@app.route('/check-listing')
def check_listing():
    req_id = request.args.get('id')
    req_date = request.args.get('date')
    rows = find_leadtime(req_id, req_date)
    
    if len(rows[0]) == 0:
        return("")
    else:
        lead_time = rows[0][0][0]
        month_avg = int(rows[1][0][0])
        return("%s,%s" %(lead_time, month_avg))
    
    
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80)
