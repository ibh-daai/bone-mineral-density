from sr_parser import convert_dicom_to_json
from datetime import datetime
from data_models import Patient, Study, Report, BMDValue, BMDTrendValue, Result, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os, glob
from prefect import task, flow, get_run_logger
from pydicom import dcmread
import pydicom.uid
from bmd_utilities import process_sample, get_report_text
from utilities import (
    create_sr,
    orthanc_get_session,
    orthanc_get_url_root,
    get_value_from_dict,
)
import pynetdicom
from pynetdicom.sop_class import (
    ComputedRadiographyImageStorage,
    DigitalXRayImageStorageForPresentation,
    SecondaryCaptureImageStorage,
    ComprehensiveSRStorage,
)
import json


@flow(name="process-bmd", log_prints=True)
def process_bmd(orthanc_study_uid):
    logger = get_run_logger()

    DATABASE_URI = os.getenv("DATABASE_URI")
    engine = create_engine(DATABASE_URI)
    Base.metadata.create_all(engine)

    series = download_study(orthanc_study_uid)
    for orthanc_series_uid in series:
        instances = download_series(orthanc_series_uid, orthanc_study_uid)
        for orthanc_instance_uid in instances:
            download_instance(orthanc_instance_uid, orthanc_study_uid)

    ## Read dicom dir and get first file as instance
    files = glob.glob(f"./{orthanc_study_uid}/*")
    if not len(files) > 0:
        logger.info(
            f"Study {orthanc_study_uid} does not contain any instances... Skipping"
        )
        return

    ## Check to see if already predicted
    accession = ""
    ds = None
    for file in files:
        ds = dcmread(file)
        predictor_root = "1.2.826.0.1.3680043.10.1082."
        if predictor_root in ds.SeriesInstanceUID:
            logger.info(f"Study {orthanc_study_uid} already processed")
            return
        if "AccessionNumber" in ds:
            accession = ds.AccessionNumber

    parse_study(orthanc_study_uid)

    Session = sessionmaker(bind=engine)
    session = Session()

    study = (
        session.query(Study).filter_by(accession=accession).first()
    )

    if study is None:
        logger.info(
            f"Study {orthanc_study_uid} was not parsed"
        )
        return

    (
        reference_examination,
        technique,
        findings,
        summary,
        diagnostic_category,
        fracture_risk,
    ) = process_sample(accession)

    sr_ds = create_sr(ds, reference_examination, technique, findings, summary)

    send_ds(sr_ds)

    generatedReport = get_report_text(
        reference_examination=reference_examination,
        technique=technique,
        findings=findings,
        summary=summary,
    )

    save_result(
        studyInstanceUID=sr_ds.StudyInstanceUID,
        patientID=ds.PatientID,
        accession=accession,
        diagnostic_category=diagnostic_category,
        fracture_risk=fracture_risk,
        generatedReport=generatedReport,
    )

    logger.info(
        f"Study {orthanc_study_uid} processed. Diagnostic Category: {diagnostic_category}, Fracture Risk: {fracture_risk}"
    )


@task(retries=3, retry_delay_seconds=5)
def download_study(orthanc_study_uid):
    logger = get_run_logger()
    orthanc_session = orthanc_get_session()
    orthanc_root = orthanc_get_url_root()

    logger.info(f"Study {orthanc_study_uid} being downloaded")

    try:
        ## Get study
        url = f"{orthanc_root}/studies/{orthanc_study_uid}"
        response = orthanc_session.get(url)
        if not response.ok:
            raise Exception(
                f"Request failed with code {response.status_code}, returned error was: {response.text}"
            )

        study = json.loads(response.text)
        series = study["Series"]
        return series

    except Exception as e:
        raise e


@task(retries=3, retry_delay_seconds=5)
def download_series(orthanc_series_uid, orthanc_study_uid):
    logger = get_run_logger()
    orthanc_session = orthanc_get_session()
    orthanc_root = orthanc_get_url_root()

    logger.info(f"Series {orthanc_series_uid} being downloaded")

    try:
        ## Get series
        url = f"{orthanc_root}/series/{orthanc_series_uid}"
        response = orthanc_session.get(url)
        if not response.ok:
            raise Exception(
                f"Request failed with code {response.status_code}, returned error was: {response.text}"
            )

        series = json.loads(response.text)
        instances = series["Instances"]
        return instances

    except Exception as e:
        raise e


@task(retries=3, retry_delay_seconds=5)
def download_instance(orthanc_instance_uid, orthanc_study_uid):
    logger = get_run_logger()
    orthanc_session = orthanc_get_session()
    orthanc_root = orthanc_get_url_root()

    logger.info(f"Instance {orthanc_instance_uid} being downloaded")

    try:
        ## Get instance
        url = f"{orthanc_root}/instances/{orthanc_instance_uid}/file"
        response = orthanc_session.get(url)
        if not response.ok:
            raise Exception(
                f"Request failed with code {response.status_code}, returned error was: {response.text}"
            )

        # Create directory for the study if it doesn't exist
        study_dir = f"./{orthanc_study_uid}"
        if not os.path.exists(study_dir):
            os.makedirs(study_dir)

        # Save the instance in the study directory
        instance_path = os.path.join(study_dir, f"{orthanc_instance_uid}.dcm")
        with open(instance_path, "wb") as instance_file:
            instance_file.write(response.content)

    except Exception as e:
        raise e


@task()
def parse_study(orthanc_study_uid):
    logger = get_run_logger()
    logger.info(f"Study {orthanc_study_uid} being parsed")

    DATABASE_URI = os.getenv("DATABASE_URI")
    engine = create_engine(DATABASE_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    files = glob.glob(f"./{orthanc_study_uid}/*")
    for file in files:
        ds = dcmread(file)

        ## Check if SR
        if not "1.2.840.10008.5.1.4.1.1.88.22" in ds.SOPClassUID:
            logger.info(f"Instance is not a SR, skipping {file}")
            continue
        try:
            data = convert_dicom_to_json(ds)
        except Exception as e:
            logger.info(f"Error parsing SR {file} due to {e}")
            continue

        sop_instance_uid = data["SOPInstanceUID"]

        report = (
            session.query(Report).filter_by(sop_instance_uid=sop_instance_uid).first()
        )

        if report:
            continue

        mrn = data["PatientID"]
        patient_age = data["PatientAge"]
        patient_sex = data["PatientSex"]
        birth_date = data["PatientBirthDate"]

        # Check if patient already exists
        patient = session.query(Patient).filter_by(mrn=mrn).first()
        if not patient:
            # Adding new patient
            patient = Patient(
                mrn=mrn,
                sex=patient_sex,
                birth_date=birth_date,
            )
            session.add(patient)
            session.commit()

        study_instance_uid = data["StudyInstanceUID"]
        accession = data["AccessionNumber"]
        study_date = datetime.strptime(data["StudyDate"], "%Y%m%d")
        study_time = datetime.strptime(data["StudyTime"], "%H%M%S").time()
        study_date_time = datetime.combine(study_date, study_time)
        study_description = data["StudyDescription"]
        study_size = (
            float(data["PatientSize"]) if data["PatientSize"] is not None else None
        )
        study_weight = (
            float(data["PatientWeight"]) if data["PatientWeight"] is not None else None
        )
        study_ethnicity = data["EthnicGroup"]
        modality = data["Modality"]
        insitution_name = data["InstitutionName"]
        station_name = data["StationName"]
        manufacturer = data["Manufacturer"]
        manufacturer_model_name = data["ManufacturerModelName"]
        software_versions = data["SoftwareVersions"]

        # Check if study already exists
        study = session.query(Study).filter_by(accession=accession).first()
        if not study:
            # Adding new study
            study = Study(
                patient_id=patient.id,
                study_instance_uid=study_instance_uid,
                accession=accession,
                date_time=study_date_time,
                description=study_description,
                age=patient_age,
                size=study_size,
                weight=study_weight,
                ethnicity=study_ethnicity,
                modality=modality,
                institution_name=insitution_name,
                station_name=station_name,
                manufacturer=manufacturer,
                manufacturer_model_name=manufacturer_model_name,
                software_versions=software_versions,
            )
            session.add(study)
            session.commit()

        if not "DXA Report" in data:
            continue

        dxa_report = data["DXA Report"]

        # Adding new report
        report = Report(
            study_id=study.id,
            sop_instance_uid=sop_instance_uid,
        )
        session.add(report)
        session.commit()

        for body_part in [
            "Left Femur",
            "AP Spine",
            "Right Femur",
            "Left Forearm",
            "Right Forearm",
            "DualFemur",
        ]:
            if body_part in dxa_report:
                body_part_data = dxa_report[body_part]
                for region_name, region_data in body_part_data.items():
                    try:
                        if "Trend" in region_name:
                            for date_str, trend_data in region_data.items():
                                age = get_value_from_dict(trend_data, ["AGE", "value"])
                                bmd = get_value_from_dict(trend_data, ["BMD", "value"])
                                change_vs_previous = get_value_from_dict(
                                    trend_data,
                                    ["CHANGE_VS_PREVIOUS", "BMD", "value"],
                                )
                                change_vs_baseline = get_value_from_dict(
                                    trend_data,
                                    ["CHANGE_VS_BASELINE", "BMD", "value"],
                                )
                                pchange_vs_previous = get_value_from_dict(
                                    trend_data,
                                    ["PCHANGE_VS_PREVIOUS", "BMD", "value"],
                                )
                                date = datetime.strptime(date_str, "%d-%b-%Y")
                                bmd_trend_value = BMDTrendValue(
                                    report_id=report.id,
                                    study_id=study.id,
                                    patient_id=patient.id,
                                    body_part=body_part,
                                    region=region_name,
                                    date=date,
                                    age=age,
                                    bmd=bmd,
                                    change_vs_previous=change_vs_previous,
                                    pchange_vs_previous=pchange_vs_previous,
                                    change_vs_baseline=change_vs_baseline,
                                )
                                session.add(bmd_trend_value)
                                session.commit()
                        else:
                            bmd = get_value_from_dict(region_data, ["BMD", "value"])
                            t_score = get_value_from_dict(region_data, ["BMD_TSCORE"])
                            z_score = get_value_from_dict(region_data, ["BMD_ZSCORE"])
                            if bmd:
                                bmd_value = BMDValue(
                                    report_id=report.id,
                                    study_id=study.id,
                                    patient_id=patient.id,
                                    body_part=body_part,
                                    region=region_name,
                                    bmd=bmd,
                                    t_score=t_score,
                                    z_score=z_score,
                                )
                                session.add(bmd_value)
                                session.commit()
                    except Exception as e:
                        logger.info(f"Error {accession} {e}")


@task(retries=3, retry_delay_seconds=5)
def send_ds(ds):
    logger = get_run_logger()
    logger.info(f"DS being sent to Orthanc")

    ae = pynetdicom.AE()

    ae.add_requested_context(
        DigitalXRayImageStorageForPresentation, pydicom.uid.ExplicitVRLittleEndian
    )

    ae.add_requested_context(
        ComputedRadiographyImageStorage, pydicom.uid.ExplicitVRLittleEndian
    )

    ae.add_requested_context(
        SecondaryCaptureImageStorage, pydicom.uid.ExplicitVRLittleEndian
    )

    ae.add_requested_context(ComprehensiveSRStorage, pydicom.uid.ExplicitVRLittleEndian)

    assoc = ae.associate("orthanc", 4242, ae_title=b"ORTHANC")
    if assoc.is_established:
        status = assoc.send_c_store(ds)
        if status:
            logger.info(
                "C-STORE succeeded request status: 0x{0:04x}".format(status.Status)
            )
        else:
            raise Exception(
                "Connection timed out, was aborted or received invalid response"
            )

        assoc.release()
    else:
        raise Exception("Association rejected, aborted or never connected")


def save_result(
    studyInstanceUID,
    patientID,
    accession,
    diagnostic_category,
    fracture_risk,
    generatedReport,
):
    DATABASE_URI = os.getenv("DATABASE_URI")
    engine = create_engine(DATABASE_URI)
    Session = sessionmaker(bind=engine)
    s = Session()
    result = Result(
        studyInstanceUID=studyInstanceUID,
        patientID=patientID,
        accession=accession,
        diagnostic_category=diagnostic_category,
        fracture_risk=fracture_risk,
        generatedReport=generatedReport,
    )
    s.add(result)
    s.commit()
