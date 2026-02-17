import os

ROW_LIMIT = 5000
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change_me_superset_secret_key")
WTF_CSRF_ENABLED = False
# Allow data upload for testing
CSV_EXTENSIONS = {"csv"}
ALLOWED_EXTENSIONS = {"csv"}
