from sr_parser import convert_dicom_to_json
from datetime import datetime
from data_models import Patient, Study, Report, BMDValue, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import glob


if __name__ == "__main__":
    # DATABASE_URI = os.getenv("RESULTS_DATABASE_CONNECTION_URL")
    # engine = create_engine(DATABASE_URI)
    engine = create_engine("sqlite:///bmd_tool.db")
    Base.metadata.create_all(engine)
    patients = glob.glob("./data/*")
    for patient in patients:
        Session = sessionmaker(bind=engine)
        session = Session()
        srs = glob.glob(f"{patient}/*/*")
        for sr in srs:
            try:
                data = convert_dicom_to_json(sr)
            except:
                continue

            sop_instance_uid = data["SOPInstanceUID"]

            report = (
                session.query(Report)
                .filter_by(sop_instance_uid=sop_instance_uid)
                .first()
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
                float(data["PatientWeight"])
                if data["PatientWeight"] is not None
                else None
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
            ]:
                if body_part in dxa_report:
                    body_part_data = dxa_report[body_part]
                    for region_name, region_data in body_part_data.items():
                        try:
                            bmd = float(region_data["BMD"]["value"])
                            t_score = float(region_data["BMD_TSCORE"])
                            z_score = float(region_data["BMD_ZSCORE"])
                            bmd_value = BMDValue(
                                report_id=report.id,
                                study_id=study.id,
                                patient_id=patient.id,
                                body_part=body_part,
                                region=region_name,
                                bmd=float(region_data["BMD"]["value"]),
                                t_score=float(region_data["BMD_TSCORE"]),
                                z_score=float(region_data["BMD_ZSCORE"]),
                            )
                            session.add(bmd_value)
                            session.commit()
                        except Exception as e:
                            continue
                            # print(f"Error {e}")
