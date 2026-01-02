"""
Script to generate password hashes for NWSFL 2025 fantasy managers

Usage:
    python scripts/generate_nwsfl_2025_logins.py
"""

import streamlit_authenticator as stauth
import secrets
import yaml

def main():
    print("NWSFL 2025 Fantasy League - Login Generator")
    print("=" * 70)
    print()
    
    # NWSFL 2025 managers with temporary passwords
    # IMPORTANT: Share these passwords with each person and have them change it!
    users = [
        ("bill", "Bill", "bill@example.com", "temppass123"),
        ("noelle", "Noelle", "noelle@example.com", "temppass123"),
        ("joey", "Joey", "joey@example.com", "temppass123"),
        ("jackson", "Jackson", "jackson@example.com", "temppass123"),
        ("ryne", "Ryne", "ryne@example.com", "temppass123"),
        ("mariam", "Mariam", "mariam@example.com", "temppass123"),
        ("danny", "Danny", "danny@example.com", "temppass123"),
        ("brock", "Brock", "brock@example.com", "temppass123"),
        ("chris", "Chris", "chris@example.com", "temppass123"),
        ("nancy", "Nancy", "nancy@example.com", "temppass123"),
        ("cameron", "Cameron", "cameron@example.com", "temppass123"),
    ]
    
    print("⚠️  TEMPORARY PASSWORDS SET TO: temppass123")
    print("   Share this with all managers and have them change it on first login!")
    print()
    
    # Extract passwords and hash them using updated API
    passwords = [pwd for _, _, _, pwd in users]
    hasher = stauth.Hasher()
    hashed_passwords = [hasher.hash(pwd) for pwd in passwords]
    
    # Generate a random cookie key
    cookie_key = secrets.token_urlsafe(32)
    
    # Build the config structure
    config = {
        'credentials': {
            'usernames': {}
        },
        'cookie': {
            'expiry_days': 30,
            'key': cookie_key,
            'name': 'nwsfl_fantasy_cookie'
        }
    }
    
    # Add all users
    for (username, name, email, _), hashed in zip(users, hashed_passwords):
        config['credentials']['usernames'][username] = {
            'email': email,
            'name': name,
            'password': hashed
        }
    
    # Print YAML format
    print("=" * 70)
    print("Copy this into your config.yaml file:")
    print("=" * 70)
    print()
    print(yaml.dump(config, default_flow_style=False, sort_keys=False))
    print()
    print("=" * 70)
    print("✅ Config generated successfully!")
    print()
    print("Next steps:")
    print("1. Copy the above YAML into config.yaml")
    print("2. Update email addresses with real ones (optional)")
    print("3. Share the temporary password (temppass123) with all managers")
    print("4. Have them change their password on first login")
    print()
    print("🔒 Cookie key has been randomly generated for security")

if __name__ == "__main__":
    main()

