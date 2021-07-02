from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests

IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'


def trigger_dag(data, context=None):
    client_id = '129604782473-6olve2vq4plj9m7ptcid2bb5m2or709n.apps.googleusercontent.com'
    webserver_id = 'https://na2e1b443c55915a7p-tp.appspot.com'
    dag_name = 'assignment_2'
    webserver_url = (
        'https://'
        + webserver_id
        + '.appspot.com/api/experimental/dags/'
        + dag_name
        + '/dag_runs'
    )
    make_iap_request(
        webserver_url, client_id, method='POST', json={"conf": data, "replace_microseconds": 'false'})


def make_iap_request(url, client_id, method='GET', **kwargs):
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 90

    google_open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    resp = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            google_open_id_connect_token)}, **kwargs)
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to '
                        'access the IAP-protected application.')
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text))
    else:
        return resp.text