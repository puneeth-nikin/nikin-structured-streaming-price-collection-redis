import logging
from kiteconnect import KiteConnect

logging.basicConfig(level=logging.DEBUG)

kite = KiteConnect(api_key="7lwrah449p8wk3i0")


data = kite.generate_session(request_token="c9ZujCZBN8juGSGOo29wpnZfVcmMUpIy", api_secret="2hcmrimx9ixwkia7on7yumtqrlcv4qn0")
print(data["access_token"])