import os

from app import create_app

app = create_app(os.getenv("FLASK_ENV") or "test")
if __name__ == "__main__":
    app.run(debug=True)
    # add port=5003 run locally
