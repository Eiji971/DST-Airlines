import requests
import time 

def get_bearer_token(client_id, client_secret):
    auth_url = 'https://developer.lufthansa.com/io-docs/getoauth2accesstoken'
    payload = {
        'apiId': 3166,
        'auth_flow': 'client_cred',
        'client_id': client_id,
        'client_secret': client_secret
    }

    response = requests.post(auth_url, data=payload)
    if response.status_code == 200:
        response_data = response.json()
        if response_data.get('success') and 'result' in response_data:
            access_token = response_data['result'].get('access_token')
            if access_token:
                return access_token

    raise Exception("Failed to obtain Bearer token.")

def get_valid_token():
    global bearer_token
    if 'bearer_token' not in globals() or not bearer_token:
        bearer_token = get_bearer_token(client_id, client_secret)

    # Check if the current token is expired
    if is_token_expired(bearer_token):
        bearer_token = get_bearer_token(client_id, client_secret)

    return bearer_token

def is_token_expired(token):
    # Decode the token to get the expiration time 
    expiration_time = time.time() + 129600  

    # Check if the current time is greater than the expiration time
    return time.time() > expiration_time


#  provided credentials.
client_id = 'pppgbsjxaegfhhh5ehjjgstnb'
client_secret = '6aHXhkBTH6'

# Get the Bearer token
bearer_token = get_bearer_token(client_id, client_secret)

headers = {
    'Authorization': f'Bearer {bearer_token}'
}


