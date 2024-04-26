# Nginx Auto Heal Bot

This project includes a Python script for monitoring the status of nodes and sending updates to a Telegram channel.

## Setup

### Requirements

- Python 3.8+
- `pip` for installing packages

### Installation

1. Clone the repository:

```
git clone https://github.com/SecretSaturn/AutoHealBot
cd AutoHealBot
```

2. Install required packages:
```
pip install aiohttp python-dotenv python-dateutil
```

3. Create a `.env` file based on the `.env.example`:
```
cp .env.example .env
```
Then, edit `.env` with your actual configuration values.

4. Create a `Nodelist.txt` file based on the `NodeList.txt.example`:
```
cp NodeList.txt.example NodeList.txt
```
Then, edit `NodeList.txt` with your actual node IPs.

### Running the Bot

To run the bot, use the following command:

```
python main.py
```

Make sure your `.env` file is correctly set up before running the script.

## License

This project is licensed under the Apache 2.0 License.

## Contributions

Contributions are welcome. Please open an issue or submit a pull request.
