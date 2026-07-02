from py_clob_client_v2 import ClobClient
import os

host = "https://clob.polymarket.com"
chain = 137  # Polygon mainnet
private_key = '019f1d32-4a3f-759f-a907-c1268bd7c5c5'
address = '0x18d26a47e4f9f976432ca7f301670c1e86f28c02'
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