import asyncio
import aiohttp
import os
import logging
from datetime import datetime, timezone
import dateutil.parser
from dotenv import load_dotenv
import subprocess  # Required for system commands
import hashlib

# Function to read URLs from file
def read_urls(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    return ["http://" + line.strip() + "/status?" for line in lines]

# Function to compute hash of the NGINX configuration to detect changes
def compute_config_hash(config_lines):
    return hashlib.sha256(''.join(config_lines).encode()).hexdigest()

# Load environment variables and read URLs
load_dotenv()
rpc_port = os.getenv('RPC_PORT', '26657')
grpc_port = os.getenv('GRPC_PORT', '9091')
lcd_port = os.getenv('LCD_PORT', '1317')
file_path = os.getenv('FILE_PATH')
nginx_config_path = os.getenv('NGINX_CONFIG_PATH')
time_before_fallen_behind = int(os.getenv('TIME_BEFORE_FALLEN_BEHIND', '30'))
base_rate = int(os.getenv('BASE_RATE', 8))  # Default to 8 if not defined in .env
node_multiplier = int(os.getenv('NODE_MULTIPLIER', 1))  # Default to 1 if not defined
update_time = int(os.getenv('UPDATE_TIME', '30'))
dry_run = os.getenv('DRY_RUN', 'false').lower() == 'true'  # Dry run option

urls = read_urls(file_path)

# Function to check nodes' health
async def check_nodes(urls):
    healthy_nodes_rpc = []
    healthy_nodes_grpc = []
    healthy_nodes_lcd = []
    datetime_now = datetime.now(timezone.utc)

    for url in urls:
        full_ip = url.split("/")[2].split(":")[0]

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=0.5) as response:
                    data = await response.json()
                    latest_block_time = data['result']['sync_info']['latest_block_time']
                    block_time = dateutil.parser.parse(latest_block_time)
                    delta_to_now = datetime_now - block_time

                    if delta_to_now.seconds <= time_before_fallen_behind:
                        healthy_nodes_rpc.append(f"{full_ip}:{rpc_port}")
                        healthy_nodes_grpc.append(f"{full_ip}:{grpc_port}")
                        healthy_nodes_lcd.append(f"{full_ip}:{lcd_port}")
        except Exception as e:
            logging.warning(f"Node unreachable: {full_ip}, Error {e}")

    return healthy_nodes_rpc, healthy_nodes_grpc, healthy_nodes_lcd

# Function to update NGINX configuration with healthy nodes and adjust rate limit
def update_nginx_config(healthy_nodes_rpc, healthy_nodes_grpc, healthy_nodes_lcd):
    # Adjust rate limit based on the total number of healthy nodes
    total_healthy_nodes = len(healthy_nodes_lcd)
    new_rate = base_rate + (total_healthy_nodes * node_multiplier)

    with open(nginx_config_path, 'r') as file:
        config_lines = file.readlines()

    # Compute current config hash to compare later
    original_hash = compute_config_hash(config_lines)

    # Update the rate limit in NGINX configuration
    for idx, line in enumerate(config_lines):
        if "limit_req_zone" in line:
            rate_limit_info = line.split()
            # Set the new rate limit
            rate_limit_info[-1] = f"rate={new_rate}r/s;"
            config_lines[idx] = " ".join(rate_limit_info) + "\n"
            break

    upstream_blocks = {
        "rpc_stream": healthy_nodes_rpc,
        "grpc_stream": healthy_nodes_grpc,
        "lcd_stream": healthy_nodes_lcd,
    }

    # Update the upstream blocks in NGINX configuration
    for upstream_name, healthy_nodes in upstream_blocks.items():
        start_index = None
        end_index = None

        # Find the start of the specific upstream block
        for idx, line in enumerate(config_lines):
            if f"upstream {upstream_name}" in line:
                start_index = idx
            elif start_index is not None and "}" in line:
                end_index = idx
                break

        # If start and end indices are found, update only the content between them
        if start_index is not None and end_index is not None:
            new_block = ["    least_conn;  # Redirect requests to the server with least number of active connections\n"]
            new_block.extend([f"    server {node} max_fails=1000 fail_timeout=30s;\n" for node in healthy_nodes])

            # Replace the inner content of the upstream block
            config_lines = (
                config_lines[:start_index + 1] + new_block + config_lines[end_index:]
            )
        else:
            logging.error(f"Upstream block '{upstream_name}' not found in the NGINX configuration.")

    # Compute new config hash
    updated_hash = compute_config_hash(config_lines)

    # Write the updated configuration only if it has changed
    if original_hash != updated_hash:
        with open(nginx_config_path, 'w') as file:
            file.writelines(config_lines)
        
        if dry_run:
            # Dry run mode: Print a message instead of reloading
            logging.info("Dry run enabled - Config changes detected, but NGINX reload is skipped.")
            subprocess.run(["whoami"], check=True)
        else:
            # Reload NGINX configuration to apply changes
            try:
                subprocess.run(["systemctl", "reload", "nginx"], check=True)
                logging.info("NGINX reloaded successfully.")
            except subprocess.CalledProcessError as e:
                logging.error("Failed to reload NGINX:", e)
    else:
        logging.info("No changes detected in NGINX configuration. Reload skipped.")

# Main function to orchestrate health checks and NGINX updates
async def main():
    while True:
        healthy_nodes_rpc, healthy_nodes_grpc, healthy_nodes_lcd = await check_nodes(urls)

        if healthy_nodes_rpc or healthy_nodes_grpc or healthy_nodes_lcd:
            update_nginx_config(healthy_nodes_rpc, healthy_nodes_grpc, healthy_nodes_lcd)

        await asyncio.sleep(update_time)

if __name__ == "__main__":
    asyncio.run(main())
