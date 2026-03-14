import sys
import os
from pathlib import Path

# Set up the path environment for Streamlit Cloud
# This ensures that 'src' and other modules are found correctly
root_dir = Path(__file__).parent.absolute()
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

# Execute the main dashboard app
# We read and execute to preserve the script's behavior without moving files
dashboard_path = root_dir / "dashboard" / "app.py"
with open(dashboard_path, "r", encoding="utf-8") as f:
    exec(f.read(), globals())
