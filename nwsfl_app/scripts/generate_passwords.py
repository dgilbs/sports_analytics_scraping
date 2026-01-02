"""
Script to generate password hashes for config.yaml

Usage:
    python scripts/generate_passwords.py
"""

import streamlit_authenticator as stauth

def main():
    print("NWSL Fantasy League - Password Hash Generator")
    print("=" * 50)
    print()
    
    # Edit this list with your friends' desired passwords
    users = [
        ("alice", "password123"),
        ("bob", "password456"),
        ("charlie", "password789"),
        # Add all 10 users here
    ]
    
    passwords = [pwd for _, pwd in users]
    hashed_passwords = stauth.Hasher(passwords).generate()
    
    print("Copy these into your config.yaml file:")
    print()
    
    for (username, _), hashed in zip(users, hashed_passwords):
        print(f"{username}:")
        print(f"  password: {hashed}")
        print()
    
    print("=" * 50)
    print("Remember to update the names and emails in config.yaml too!")

if __name__ == "__main__":
    main()