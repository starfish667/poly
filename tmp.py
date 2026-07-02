from py_clob_client_v2 import ClobClient
import os

host = "https://clob.polymarket.com"
chain = 137  # Polygon mainnet
private_key = os.getenv("PRIVATE_KEY")
address = '0x9A8BE095b57163132959A5D054e4F90BE76c015D'
print(private_key)
# Derive API credentials (L1 → L2 auth)
temp_client = ClobClient(host, key=private_key, chain_id=chain)
api_creds = temp_client.create_or_derive_api_key()

# Initialize trading client
client = ClobClient(
    host,
    key=private_key,
    chain_id=chain,
    creds=api_creds,
    signature_type=0,  # Signature type: 0 = EOA
    funder=address,  # Funder address
)