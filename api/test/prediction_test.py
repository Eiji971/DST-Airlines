import requests
import os

api_adress = "api"
api_port = 8000

def test_prediction(username, password, filepath):
    url = 'http://{address}:{port}/predict'.format(adress=api_adress, port=api_port)
    params = {'username': username, 'password': password, 'filepath': ''}

    output = '''
============================
    Content Test
============================

username: {username}
password: {password}

'''

    params['filepath'] = filepath
    response = requests.get(url, params=params)
    result = response.json()

    output += 'Result: {prediction}'
    print(output.format(username=username, password=password,
                            prediction=result))

    if os.environ.get('LOG') == '1':
        with open('log.txt', 'a') as file:
            file.write(output.format(username=username, password=password, sentence=sentence,
                                     score_v1=result_v1['score'], score_v2=result_v2['score']))
    output = ''

filepath = '/Users/emerybosc/DST-Airlines/data/extractedcsv/flight_status2023-07-18.csv'
test_prediction('emgri', 'BootcampDE0523', filepath)