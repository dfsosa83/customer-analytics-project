import os
import stat

def setup_snowflake_permissions():
    # Create .snowflake directory if it doesn't exist
    snowflake_dir = os.path.expanduser('~/.snowflake')
    os.makedirs(snowflake_dir, exist_ok=True)

    # Create or modify connections.toml with correct permissions
    connections_path = os.path.join(snowflake_dir, 'connections.toml')
    if not os.path.exists(connections_path):
        with open(connections_path, 'w') as f:
            f.write('')
    
    # Set correct permissions (read/write for owner only)
    os.chmod(connections_path, stat.S_IRUSR | stat.S_IWUSR)
    print("Snowflake permissions setup completed successfully!")

if __name__ == "__main__":
    setup_snowflake_permissions()
