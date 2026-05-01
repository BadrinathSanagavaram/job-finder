"""
One-time OAuth2 setup — run this once to store token.json.
Opens a browser window for Google login with badrinath.sanagavaram@gmail.com.
After authorization, token.json is saved next to this file.
"""
import os
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

BASE       = os.path.dirname(os.path.abspath(__file__))
CLIENT_FILE = os.path.join(BASE, "oauth_client.json")
TOKEN_FILE  = os.path.join(BASE, "token.json")


def main():
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_FILE, scopes=SCOPES)
    creds = flow.run_local_server(port=0)

    with open(TOKEN_FILE, "w") as f:
        f.write(creds.to_json())

    print(f"\ntoken.json saved to: {TOKEN_FILE}")
    print("OAuth2 setup complete — you can now run main.py")


if __name__ == "__main__":
    main()
