import os
from requests.auth import HTTPBasicAuth
from dicomweb_client.session_utils import create_session_from_auth


def orthanc_get_session():
    user = os.environ.get("ORTHANC_API_USER")
    if not user:
        user = "orthanc"

    password = os.environ.get("ORTHANC_API_PASSWORD")
    if not password:
        password = "orthanc"

    orthanc_auth = HTTPBasicAuth(user, password)
    orthanc_session = create_session_from_auth(orthanc_auth)
    return orthanc_session


def orthanc_get_url_root():
    return "http://orthanc:8042"
