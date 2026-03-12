# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 16:42:19 2026

@author: Joseph Montoya
"""

# import subprocess
# subprocess.run(['pip', 'install', 'google-auth-oauthlib'], check=True)

pip install google-auth-oauthlib
#%%


# pip install google-auth-oauthlib
from google_auth_oauthlib.flow import InstalledAppFlow
import json

#%%

flow = InstalledAppFlow.from_client_secrets_file(
    r'C:/Users/Joseph Montoya/Downloads/oauth_client.json', # este el json que hemos descargado
    scopes=['https://www.googleapis.com/auth/drive'])

creds = flow.run_local_server(port=0)  # Abre el navegador para que apruebes

#%%
# Genera el user_token.json
token_data = {
    'token': creds.token,
    'refresh_token': creds.refresh_token,
    'token_uri': creds.token_uri,
    'client_id': creds.client_id,
    'client_secret': creds.client_secret,
}
with open(r'C:/Users/Joseph Montoya/Downloads/oauth_client.json', 'w') as f:
    json.dump(token_data, f)

print("✅ user_token.json generado")

