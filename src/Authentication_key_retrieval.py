import requests
import time 

def get_bearer_token(client_id = 'pppgbsjxaegfhhh5ehjjgstnb', client_secret = '6aHXhkBTH6'):
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

def get_valid_token(bearer_token=None):
    if bearer_token is None or is_token_expired(bearer_token):
        return get_bearer_token()

    return bearer_token

def is_token_expired(token):
    # Decode the token to get the expiration time
    # Assuming the token has an "exp" claim indicating the expiration time in seconds
    expiration_time = token.get('exp', 0)

    # Check if the current time is greater than the expiration time
    if time.time() > expiration_time:
        return True
    return False





bearer_token = get_valid_token()
# Information on the headers for API calls
headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Accept": "application/json"
    }
print(headers)