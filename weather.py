import sys


from flask import Flask, request, abort, jsonify
import requests
from urllib3.exceptions import HTTPError
import re
from datetime import datetime, timedelta
if sys.version_info[0] < 3:
    import time
from dateutil import parser
from dateutil.tz import tzlocal


# https://api.darksky.net/forecast/2090b1620adedfaabf9d1667a646c9bb/33.7073908,-117.7666567,1508490000?exclude=currently,minutely,hourly,flags

DARKSKY_URL = 'https://api.darksky.net/forecast/'
DARKSKY_API_KEY = '2090b1620adedfaabf9d1667a646c9bb'
EXCLUDE_BLOCKS = ['currently', 'minutely', 'hourly', 'flags']

WHEATHER_SERVICE_URL = '/weather/api/'
WHEATHER_SERVICE_API_VERSION = '1.0'

LOCATION_SERVICE_API = '1.0'
LOCATION_SERVICE_URL = 'weather/api/' + LOCATION_SERVICE_API + '/addresses/convert'
LOCATION_SERVICE_PORT = 5001  # assume deployment on the same host
HOST_URL_RE = r'^(http\:\/\/.+\:)\d{2,5}(\/)$'

url_re = re.compile(HOST_URL_RE)


app = Flask(__name__)


@app.route(WHEATHER_SERVICE_URL + WHEATHER_SERVICE_API_VERSION + '/reports/history', methods=['GET'])
def get_weather_report():
    # address is in the form 'lat,lng'
    address = request.args.get('address')

    # the date from which to get the historical weather reports, going back <w> weeks
    delta = timedelta(days=1)
    date = request.args.get('date', (datetime.now() - delta).strftime('%x'))
    # normalize it to mid day (12PM)
    try:
        dt = parser.parse(date)
        mid_day_dt = datetime(dt.year, dt.month, dt.day, hour=12, tzinfo=tzlocal())
    except ValueError as e:
        abort(400, str(e))

    # number of weeks before the <date> to retrieve the historical weather reports
    # default is 1 week, up to 4
    try:
        weeks = int(request.args.get('w', 1))
        if weeks not in [1, 2, 3, 4]:
            raise ValueError("'w' must be an integer between 1 and 4")
    except ValueError as e:
        abort(400, str(e))

    # retrieve the geo coordinates using our location microservice
    # host_url = url_re.sub(r'\g<1>' + str(LOCATION_SERVICE_PORT) + r'\g<2>', request.host_url)
    # coord = requests.get(host_url + LOCATION_SERVICE_URL, params={'address': address})

    data_points = []
    data_points_count = weeks * 7

    # seems that this DarkSky request only returns daily historical report for 1 day only
    # loop for all days from most recent one
    for i in range(data_points_count):
        mid_day_dt -= i * delta
        if sys.version_info[0] < 3:
            tstamp = time.mktime(mid_day_dt.timetuple())
        else:
            tstamp = mid_day_dt.timestamp()

        path_args = address.split(',')
        # path_args = [str(x) for x in coord.json()['location'].values()]
        path_args.append(str(int(tstamp)))
        data = None
        try:
            rsp = requests.get(DARKSKY_URL + DARKSKY_API_KEY + '/' + ",".join(path_args),
                               params={'exclude': ",".join(EXCLUDE_BLOCKS)})

            if rsp is not None:
                data = rsp.json()
                if data is not None:
                    data = data['daily']['data'][0]
        except HTTPError as e:
            app.logger.error("HTTP error: %s".format(str(e)))

        data_points.append(data)

    return jsonify({'data': data_points})


if __name__ == '__main__':
    app.run()
