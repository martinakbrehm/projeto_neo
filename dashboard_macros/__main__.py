import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

try:
    from .dashboard import app
except ImportError:
    from dashboard import app

if __name__ == '__main__':
    # roda o app com valores default
    app.run(host='0.0.0.0', port=8050, debug=False, use_reloader=False)
