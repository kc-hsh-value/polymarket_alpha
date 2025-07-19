# import markets.json
from pathlib import Path
import json

markets = json.load(open(Path(__file__).parent / "markets.json"))

print(len(markets))

