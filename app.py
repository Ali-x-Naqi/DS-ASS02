import sys
import os
from pathlib import Path

# Set up the path environment for Streamlit Cloud
# This ensures that 'src' and other modules are found correctly
root_dir = Path(__file__).parent.absolute()
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

# Execute the main dashboard app with corrected file context
# This ensures that Path(__file__).parent.parent resolves correctly to the project root
dashboard_path = root_dir / "dashboard" / "app.py"
with open(dashboard_path, "r", encoding="utf-8") as f:
    # We pass a custom global dict to fix the __file__ resolution inside the dashboard
    custom_globals = {
        "__file__": str(dashboard_path),
        "__name__": "__main__",
        "__builtins__": __builtins__
    }
    exec(f.read(), custom_globals)
