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
        ("bill", "bill2025!"),
        ("noelle", "noelle2025!"),
        ("joey", "joey2025!"),
        ("jackson", "jackson2025!"),
        ("ryne", "ryne2025!"),
        ("mariam", "mariam2025!"),
        ("danny", "danny2025!"),
        ("brock", "brock2025!"),
        ("chris", "chris2025!"),
        ("nancy", "nancy2025!"),
        ("cameron", "cameron2025!"),
    ]
    
    print("Copy these into your config.yaml file:")
    print()
    
    hasher = stauth.Hasher()
    for username, password in users:
        hashed = hasher.hash(password)
        print(f"{username}:")
        print(f"  password: {hashed}")
        print()
    
    print("=" * 50)
    print("Remember to update the names and emails in config.yaml too!")

if __name__ == "__main__":
    main()