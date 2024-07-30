import os
from requests.auth import HTTPBasicAuth
from dicomweb_client.session_utils import create_session_from_auth
import pydicom
from pydicom.dataset import Dataset, FileDataset
from pydicom.uid import generate_uid, ExplicitVRLittleEndian, CTImageStorage
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


# Function to create a basic SR document
def create_sr(ds, reference_examination, technique, findings, summary):
    # Create a new FileDataset instance (instance of Dataset)
    file_meta = Dataset()
    file_meta.MediaStorageSOPClassUID = generate_uid()
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    file_meta.ImplementationClassUID = generate_uid()

    # Create the FileDataset instance
    sr_ds = FileDataset("sr.dcm", {}, file_meta=file_meta, preamble=b"\0" * 128)

    # Set the necessary metadata
    sr_ds.ContentDate = datetime.now().strftime("%Y%m%d")
    sr_ds.ContentTime = datetime.now().strftime("%H%M%S")
    sr_ds.InstanceCreationDate = sr_ds.ContentDate
    sr_ds.InstanceCreationTime = sr_ds.ContentTime
    sr_ds.Modality = "SR"
    sr_ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.88.11"
    sr_ds.SOPInstanceUID = generate_uid(f"1.2.826.0.1.3680043.10.1082.2." + "3.")
    sr_ds.SeriesInstanceUID = generate_uid(f"1.2.826.0.1.3680043.10.1082.2." + "3.")
    sr_ds.SeriesNumber = "3"
    sr_ds.StudyInstanceUID = ds.StudyInstanceUID
    sr_ds.PatientID = ds.PatientID
    sr_ds.PatientName = ds.PatientName
    sr_ds.PatientBirthDate = ds.PatientBirthDate
    sr_ds.PatientSex = ds.PatientSex

    # Add content sequence with basic SR structure
    sr_ds.ContentSequence = Sequence()

    # Reference Examination
    content_item = Dataset()
    content_item.ValueType = "TEXT"
    content_item.ConceptNameCodeSequence = Sequence([Dataset()])
    content_item.ConceptNameCodeSequence[0].CodeValue = "0-0-0"
    content_item.ConceptNameCodeSequence[0].CodingSchemeDesignator = "AIDE"
    content_item.ConceptNameCodeSequence[0].CodeMeaning = "Reference Examination"
    content_item.TextValue = reference_examination
    sr_ds.ContentSequence.append(content_item)

    # Technique
    content_item = Dataset()
    content_item.ValueType = "TEXT"
    content_item.ConceptNameCodeSequence = Sequence([Dataset()])
    content_item.ConceptNameCodeSequence[0].CodeValue = "0-0-1"
    content_item.ConceptNameCodeSequence[0].CodingSchemeDesignator = "AIDE"
    content_item.ConceptNameCodeSequence[0].CodeMeaning = "Technique"
    content_item.TextValue = technique
    sr_ds.ContentSequence.append(content_item)

    # Findings
    content_item = Dataset()
    content_item.ValueType = "TEXT"
    content_item.ConceptNameCodeSequence = Sequence([Dataset()])
    content_item.ConceptNameCodeSequence[0].CodeValue = "0-0-2"
    content_item.ConceptNameCodeSequence[0].CodingSchemeDesignator = "AIDE"
    content_item.ConceptNameCodeSequence[0].CodeMeaning = "Findings"
    content_item.TextValue = findings
    sr_ds.ContentSequence.append(content_item)

    # Summary
    content_item = Dataset()
    content_item.ValueType = "TEXT"
    content_item.ConceptNameCodeSequence = Sequence([Dataset()])
    content_item.ConceptNameCodeSequence[0].CodeValue = "0-0-3"
    content_item.ConceptNameCodeSequence[0].CodingSchemeDesignator = "AIDE"
    content_item.ConceptNameCodeSequence[0].CodeMeaning = "Summary"
    content_item.TextValue = summary
    sr_ds.ContentSequence.append(content_item)

    # Add reference to the original study
    sr_ds.ReferencedStudySequence = Sequence([Dataset()])
    sr_ds.ReferencedStudySequence[0].ReferencedSOPClassUID = ds.SOPClassUID
    sr_ds.ReferencedStudySequence[0].ReferencedSOPInstanceUID = ds.SOPInstanceUID

    return sr_ds


def get_value_from_dict(data_dict, keys):
    """Extract value from nested dictionary if keys exist, return None otherwise."""
    for key in keys:
        if key in data_dict:
            data_dict = data_dict[key]
        else:
            return None
    return data_dict
