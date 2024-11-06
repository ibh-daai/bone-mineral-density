import os
from requests.auth import HTTPBasicAuth
from dicomweb_client.session_utils import create_session_from_auth
import pydicom
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.uid import generate_uid, ExplicitVRLittleEndian, ComprehensiveSRStorage
from pydicom.sequence import Sequence
from datetime import datetime


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


def add_if_exists(ds: Dataset, field: str):
    """Checks if a field exists in a DICOM dataset and returns its value, otherwise returns 0.

    Args:
        ds (pydicom.dataset.FileDataset): The DICOM dataset to search for the field.
        field (str): The name of the field to search for.

    Returns:
        The value of the field if it exists in the dataset, otherwise 0.
    """
    if field in ds:
        return ds[field].value
    else:
        return ""


# Function to create a basic SR document
def create_sr(ds, findings, diagnostic_category):
    # Create a new FileDataset instance (instance of Dataset)
    predictor_uid_root = "1.2.826.0.1.3680043.10.1082."
    series_num = 3
    sop_uid = generate_uid(f"{predictor_uid_root}2.{series_num}.")
    series_uid = generate_uid(f"{predictor_uid_root}2.{series_num}.")

    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = ComprehensiveSRStorage
    file_meta.MediaStorageSOPInstanceUID = sop_uid
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian

    # Create the FileDataset instance
    sr_ds = Dataset()

    # Set the necessary metadata
    sr_ds.Modality = "SR"
    sr_ds.SOPClassUID = ComprehensiveSRStorage
    sr_ds.SOPInstanceUID = sop_uid
    sr_ds.SeriesInstanceUID = series_uid
    sr_ds.SeriesNumber = "3"
    sr_ds.InstanceNumber = "1"

    sr_ds.ContentDate = datetime.now().strftime("%Y%m%d")
    sr_ds.ContentTime = datetime.now().strftime("%H%M%S")
    sr_ds.InstanceCreationDate = sr_ds.ContentDate
    sr_ds.InstanceCreationTime = sr_ds.ContentTime
    sr_ds.StudyInstanceUID = add_if_exists(ds, "StudyInstanceUID")
    sr_ds.PatientID = add_if_exists(ds, "PatientID")
    sr_ds.PatientName = add_if_exists(ds, "PatientName")
    sr_ds.PatientBirthDate = add_if_exists(ds, "PatientBirthDate")
    sr_ds.PatientSex = add_if_exists(ds, "PatientSex")

    # Add content sequence with basic SR structure
    sr_ds.ContentSequence = Sequence()

    # findings
    content_item = Dataset()
    content_item.ValueType = "TEXT"
    content_item.ConceptNameCodeSequence = Sequence([Dataset()])
    content_item.ConceptNameCodeSequence[0].CodeValue = "0-0-1"
    content_item.ConceptNameCodeSequence[0].CodingSchemeDesignator = "AIDE"
    content_item.ConceptNameCodeSequence[0].CodeMeaning = "FINDINGS"
    content_item.TextValue = findings
    sr_ds.ContentSequence.append(content_item)

    # diagnostic_category
    content_item = Dataset()
    content_item.ValueType = "TEXT"
    content_item.ConceptNameCodeSequence = Sequence([Dataset()])
    content_item.ConceptNameCodeSequence[0].CodeValue = "0-0-2"
    content_item.ConceptNameCodeSequence[0].CodingSchemeDesignator = "AIDE"
    content_item.ConceptNameCodeSequence[0].CodeMeaning = "DIAGNOSTIC CATEGORY"
    content_item.TextValue = diagnostic_category
    sr_ds.ContentSequence.append(content_item)

    # Add reference to the original study
    sr_ds.ReferencedStudySequence = Sequence([Dataset()])
    sr_ds.ReferencedStudySequence[0].ReferencedSOPClassUID = ds.SOPClassUID
    sr_ds.ReferencedStudySequence[0].ReferencedSOPInstanceUID = ds.SOPInstanceUID

    sr_ds.fix_meta_info()
    sr_ds.file_meta = file_meta
    sr_ds.is_implicit_VR = False
    sr_ds.is_little_endian = True

    return sr_ds


def get_value_from_dict(data_dict, keys):
    """Extract value from nested dictionary if keys exist, return None otherwise."""
    for key in keys:
        if key in data_dict:
            data_dict = data_dict[key]
        else:
            return None
    return data_dict
